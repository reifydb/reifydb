// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	mem,
	ops::Deref,
	sync::{Arc, RwLock},
};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_type::value::sumtype::VariantRef;

use super::{
	FunctionKind, Routine,
	context::{FunctionContext, ProcedureContext},
};

/// Trait-object alias for a function routine.
///
/// `for<'a> Routine<FunctionContext<'a>>` reads as "implements Routine for
/// every choice of `'a`" — required because `FunctionContext<'a>` carries
/// borrowed catalog/runtime/IOC handles whose lifetime is determined by the
/// dispatch site, not the registration site.
pub type DynFunction = dyn for<'a> Routine<FunctionContext<'a>>;

/// Trait-object alias for a procedure routine.
pub type DynProcedure = dyn for<'a, 'tx> Routine<ProcedureContext<'a, 'tx>>;

/// Unified registry for all routines (functions and procedures).
///
/// Internally splits by context type because `Routine<FunctionContext>` and
/// `Routine<ProcedureContext>` are different trait-object types — there is no
/// way to put both in a single map. Callers see one `Routines` handle with one
/// public API; the split is invisible above this module.
#[derive(Clone)]
pub struct Routines(Arc<RoutinesInner>);

impl Routines {
	pub fn empty() -> Routines {
		Routines::builder().configure()
	}

	pub fn builder() -> RoutinesConfigurator {
		RoutinesConfigurator {
			functions: HashMap::new(),
			procedures: HashMap::new(),
			deferred_handlers: Vec::new(),
		}
	}
}

impl Deref for Routines {
	type Target = RoutinesInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct RoutinesInner {
	functions: HashMap<String, Arc<DynFunction>>,
	procedures: HashMap<String, Arc<DynProcedure>>,
	handlers: RwLock<EventHandlerState>,
}

struct EventHandlerState {
	resolved: HashMap<VariantRef, Vec<Arc<DynProcedure>>>,
	deferred: Vec<(String, Arc<DynProcedure>)>,
}

impl RoutinesInner {
	pub fn get_function(&self, name: &str) -> Option<Arc<DynFunction>> {
		self.functions.get(name).cloned()
	}

	pub fn get_procedure(&self, name: &str) -> Option<Arc<DynProcedure>> {
		self.procedures.get(name).cloned()
	}

	pub fn has_function(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedures.contains_key(name)
	}

	pub fn get_scalar_function(&self, name: &str) -> Option<Arc<DynFunction>> {
		self.functions.get(name).cloned().filter(|f| f.kinds().contains(&FunctionKind::Scalar))
	}

	pub fn get_aggregate_function(&self, name: &str) -> Option<Arc<DynFunction>> {
		self.functions.get(name).cloned().filter(|f| f.kinds().contains(&FunctionKind::Aggregate))
	}

	pub fn get_generator_function(&self, name: &str) -> Option<Arc<DynFunction>> {
		self.functions.get(name).cloned().filter(|f| f.kinds().contains(&FunctionKind::Generator))
	}

	pub fn function_names(&self) -> Vec<&str> {
		self.functions.keys().map(|s| s.as_str()).collect()
	}

	pub fn procedure_names(&self) -> Vec<String> {
		self.procedures.keys().cloned().collect()
	}

	pub fn scalar_function_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Scalar))
			.map(|(s, _)| s.as_str())
			.collect()
	}

	pub fn aggregate_function_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Aggregate))
			.map(|(s, _)| s.as_str())
			.collect()
	}

	pub fn generator_function_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Generator))
			.map(|(s, _)| s.as_str())
			.collect()
	}

	/// Resolve event handlers bound to a sumtype variant. Lazy: deferred
	/// handlers are resolved on first dispatch using the materialized catalog
	/// to look up their target `VariantRef`.
	pub fn get_handlers(&self, catalog: &MaterializedCatalog, variant: VariantRef) -> Vec<Arc<DynProcedure>> {
		{
			let mut state = self.handlers.write().unwrap();
			if !state.deferred.is_empty() {
				let deferred = mem::take(&mut state.deferred);
				let mut still_deferred = Vec::new();
				for (path, handler) in deferred {
					match resolve_event_path(&path, catalog) {
						Ok(resolved) => {
							state.resolved.entry(resolved).or_default().push(handler);
						}
						Err(_) => still_deferred.push((path, handler)),
					}
				}
				state.deferred = still_deferred;
			}
		}
		let state = self.handlers.read().unwrap();
		state.resolved.get(&variant).map(|hs| hs.to_vec()).unwrap_or_default()
	}
}

pub struct RoutinesConfigurator {
	functions: HashMap<String, Arc<DynFunction>>,
	procedures: HashMap<String, Arc<DynProcedure>>,
	deferred_handlers: Vec<(String, Arc<DynProcedure>)>,
}

impl RoutinesConfigurator {
	pub fn has_function(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedures.contains_key(name)
	}

	/// Register a function. The impl block (`impl Routine<FunctionContext>`)
	/// determines that this is a function rather than a procedure.
	pub fn register_function(mut self, routine: Arc<DynFunction>) -> Self {
		self.functions.insert(routine.info().name.clone(), routine);
		self
	}

	/// Register a procedure. The impl block (`impl Routine<ProcedureContext>`)
	/// determines that this is a procedure rather than a function.
	pub fn register_procedure(mut self, routine: Arc<DynProcedure>) -> Self {
		self.procedures.insert(routine.info().name.clone(), routine);
		self
	}

	/// Register an event handler procedure by sumtype-variant path.
	///
	/// `event_path` uses the format `"namespace::event_name::VariantName"`.
	/// Resolution is lazy — the handler is resolved on first dispatch.
	pub fn register_handler(mut self, event_path: &str, routine: Arc<DynProcedure>) -> Self {
		self.deferred_handlers.push((event_path.to_string(), routine));
		self
	}

	pub fn register_function_alias(mut self, alias: &str, canonical: &str) -> Self {
		if let Some(routine) = self.functions.get(canonical).cloned() {
			self.functions.insert(alias.to_string(), routine);
		}
		self
	}

	pub fn register_procedure_alias(mut self, alias: &str, canonical: &str) -> Self {
		if let Some(routine) = self.procedures.get(canonical).cloned() {
			self.procedures.insert(alias.to_string(), routine);
		}
		self
	}

	pub fn configure(self) -> Routines {
		Routines(Arc::new(RoutinesInner {
			functions: self.functions,
			procedures: self.procedures,
			handlers: RwLock::new(EventHandlerState {
				resolved: HashMap::new(),
				deferred: self.deferred_handlers,
			}),
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
