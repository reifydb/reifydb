// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	mem,
	ops::Deref,
	sync::{Arc, Mutex},
};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_type::value::sumtype::VariantRef;

use super::Procedure;

type ProcedureFactory = Arc<dyn Fn() -> Box<dyn Procedure> + Send + Sync>;

#[derive(Clone)]
pub struct Procedures(Arc<ProceduresInner>);

impl Procedures {
	pub fn empty() -> Procedures {
		Procedures::builder().build()
	}

	pub fn builder() -> ProceduresBuilder {
		ProceduresBuilder {
			procedures: HashMap::new(),
			deferred_handlers: Vec::new(),
		}
	}
}

impl Deref for Procedures {
	type Target = ProceduresInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

struct RegistryState {
	procedures: HashMap<String, ProcedureFactory>,
	resolved_handlers: HashMap<VariantRef, Vec<ProcedureFactory>>,
	deferred_handlers: Vec<(String, ProcedureFactory)>,
}

pub struct ProceduresInner {
	state: Arc<Mutex<RegistryState>>,
}

impl Clone for ProceduresInner {
	fn clone(&self) -> Self {
		Self {
			state: Arc::clone(&self.state),
		}
	}
}

impl ProceduresInner {
	pub fn get_procedure(&self, name: &str) -> Option<Box<dyn Procedure>> {
		self.state.lock().unwrap().procedures.get(name).map(|f| f())
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.state.lock().unwrap().procedures.contains_key(name)
	}

	pub fn get_handlers(&self, catalog: &MaterializedCatalog, variant: VariantRef) -> Vec<Box<dyn Procedure>> {
		let mut state = self.state.lock().unwrap();
		if !state.deferred_handlers.is_empty() {
			let deferred = mem::take(&mut state.deferred_handlers);
			let mut still_deferred = Vec::new();
			for (path, factory) in deferred {
				match resolve_event_path(&path, catalog) {
					Ok(resolved) => {
						state.resolved_handlers.entry(resolved).or_default().push(factory);
					}
					Err(_) => still_deferred.push((path, factory)),
				}
			}
			state.deferred_handlers = still_deferred;
		}
		state.resolved_handlers
			.get(&variant)
			.map(|factories| factories.iter().map(|f| f()).collect())
			.unwrap_or_default()
	}
}

pub struct ProceduresBuilder {
	procedures: HashMap<String, ProcedureFactory>,
	deferred_handlers: Vec<(String, ProcedureFactory)>,
}

impl ProceduresBuilder {
	pub fn with_procedure<F, P>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> P + Send + Sync + 'static,
		P: Procedure + 'static,
	{
		self.procedures.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Procedure>));

		self
	}

	/// Register an event handler by path.
	///
	/// `event_path` uses the format `"namespace::event_name::VariantName"`.
	/// The handler is resolved lazily on first dispatch.
	pub fn with_handler<F, P>(mut self, event_path: &str, init: F) -> Self
	where
		F: Fn() -> P + Send + Sync + 'static,
		P: Procedure + 'static,
	{
		self.deferred_handlers
			.push((event_path.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Procedure>)));
		self
	}

	pub fn build(self) -> Procedures {
		Procedures(Arc::new(ProceduresInner {
			state: Arc::new(Mutex::new(RegistryState {
				procedures: self.procedures,
				resolved_handlers: HashMap::new(),
				deferred_handlers: self.deferred_handlers,
			})),
		}))
	}
}

fn resolve_event_path(path: &str, catalog: &MaterializedCatalog) -> Result<VariantRef, String> {
	let parts: Vec<&str> = path.split("::").collect();
	if parts.len() != 3 {
		return Err(format!(
			"Invalid event path '{}': expected format 'namespace::event_name::VariantName'",
			path
		));
	}
	let (namespace_name, event_name, variant_name) = (parts[0], parts[1], parts[2]);

	let namespace = catalog
		.find_namespace_by_name(namespace_name)
		.ok_or_else(|| format!("Namespace '{}' not found", namespace_name))?;

	let sumtype = catalog
		.find_sumtype_by_name(namespace.id(), event_name)
		.ok_or_else(|| format!("SumType '{}' not found in namespace '{}'", event_name, namespace_name))?;

	let variant_name_lower = variant_name.to_lowercase();
	let variant = sumtype.variants.iter().find(|v| v.name == variant_name_lower).ok_or_else(|| {
		format!("Variant '{}' not found in sumtype '{}::{}'", variant_name, namespace_name, event_name)
	})?;

	Ok(VariantRef {
		sumtype_id: sumtype.id,
		variant_tag: variant.tag,
	})
}
