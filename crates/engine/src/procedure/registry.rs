// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_type::value::sumtype::SumTypeId;

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
			inner: ProceduresInner {
				procedures: HashMap::new(),
				handlers: HashMap::new(),
			},
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

#[derive(Clone)]
pub struct ProceduresInner {
	procedures: HashMap<String, ProcedureFactory>,
	handlers: HashMap<(SumTypeId, u8), Vec<ProcedureFactory>>,
}

impl ProceduresInner {
	pub fn get_procedure(&self, name: &str) -> Option<Box<dyn Procedure>> {
		self.procedures.get(name).map(|func| func())
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedures.contains_key(name)
	}

	pub fn get_handlers(&self, sumtype_id: SumTypeId, variant_tag: u8) -> Vec<Box<dyn Procedure>> {
		self.handlers
			.get(&(sumtype_id, variant_tag))
			.map(|factories| factories.iter().map(|f| f()).collect())
			.unwrap_or_default()
	}
}

pub struct ProceduresBuilder {
	inner: ProceduresInner,
	deferred_handlers: Vec<(String, ProcedureFactory)>,
}

impl ProceduresBuilder {
	pub fn with_procedure<F, P>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> P + Send + Sync + 'static,
		P: Procedure + 'static,
	{
		self.inner
			.procedures
			.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Procedure>));

		self
	}

	/// Register an event handler by path.
	///
	/// `event_path` uses the format `"namespace::event_name::VariantName"`.
	/// The handler is deferred until `resolve()` is called with a loaded catalog.
	pub fn with_handler<F, P>(mut self, event_path: &str, init: F) -> Self
	where
		F: Fn() -> P + Send + Sync + 'static,
		P: Procedure + 'static,
	{
		self.deferred_handlers
			.push((event_path.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Procedure>)));
		self
	}

	/// Resolve deferred handlers against the loaded catalog.
	pub fn resolve(mut self, catalog: &MaterializedCatalog) -> Result<Self, String> {
		let deferred = std::mem::take(&mut self.deferred_handlers);
		for (event_path, factory) in deferred {
			let (sumtype_id, variant_tag) = resolve_event_path(&event_path, catalog)?;
			self.inner.handlers.entry((sumtype_id, variant_tag)).or_default().push(factory);
		}
		Ok(self)
	}

	pub fn build(self) -> Procedures {
		Procedures(Arc::new(self.inner))
	}
}

fn resolve_event_path(path: &str, catalog: &MaterializedCatalog) -> Result<(SumTypeId, u8), String> {
	let parts: Vec<&str> = path.split("::").collect();
	if parts.len() != 3 {
		return Err(format!(
			"Invalid event path '{}': expected format 'namespace::event_name::VariantName'",
			path
		));
	}
	let (namespace_name, event_name, variant_name) = (parts[0], parts[1], parts[2]);

	let namespace_def = catalog
		.find_namespace_by_name(namespace_name)
		.ok_or_else(|| format!("Namespace '{}' not found", namespace_name))?;

	let sumtype_def = catalog
		.find_sumtype_by_name(namespace_def.id, event_name)
		.ok_or_else(|| format!("SumType '{}' not found in namespace '{}'", event_name, namespace_name))?;

	let variant = sumtype_def.variants.iter().find(|v| v.name == variant_name).ok_or_else(|| {
		format!("Variant '{}' not found in sumtype '{}::{}'", variant_name, namespace_name, event_name)
	})?;

	Ok((sumtype_def.id, variant.tag))
}
