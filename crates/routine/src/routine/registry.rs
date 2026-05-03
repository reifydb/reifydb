// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem,
	ops::Deref,
	sync::{Arc, RwLock},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::VariantRef;

use super::{Function, FunctionKind, Procedure};

pub const BUILTIN_FUNCTION_PREFIX: &str = "system::builtin::functions::";

pub const BUILTIN_PROCEDURE_PREFIX: &str = "system::builtin::procedures::";

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
	functions: HashMap<String, Arc<dyn Function>>,
	procedures: HashMap<String, Arc<dyn Procedure>>,
	builtin_function_namespaces: HashSet<String>,
	builtin_procedure_namespaces: HashSet<String>,
	handlers: RwLock<EventHandlerState>,
}

struct EventHandlerState {
	resolved: HashMap<VariantRef, Vec<Arc<dyn Procedure>>>,
	deferred: Vec<(String, Arc<dyn Procedure>)>,
}

impl RoutinesInner {
	pub fn get_function(&self, name: &str) -> Option<Arc<dyn Function>> {
		if let Some(f) = self.functions.get(name) {
			return Some(f.clone());
		}
		let first_segment = name.split_once("::").map(|(ns, _)| ns)?;
		if self.builtin_function_namespaces.contains(first_segment) {
			let canonical = format!("{BUILTIN_FUNCTION_PREFIX}{name}");
			return self.functions.get(&canonical).cloned();
		}
		None
	}

	pub fn get_procedure(&self, name: &str) -> Option<Arc<dyn Procedure>> {
		if let Some(p) = self.procedures.get(name) {
			return Some(p.clone());
		}
		let first_segment = name.split_once("::").map(|(ns, _)| ns)?;
		if self.builtin_procedure_namespaces.contains(first_segment) {
			let canonical = format!("{BUILTIN_PROCEDURE_PREFIX}{name}");
			return self.procedures.get(&canonical).cloned();
		}
		None
	}

	pub fn has_function(&self, name: &str) -> bool {
		self.get_function(name).is_some()
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.get_procedure(name).is_some()
	}

	pub fn get_scalar_function(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.get_function(name).filter(|f| f.kinds().contains(&FunctionKind::Scalar))
	}

	pub fn get_aggregate_function(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.get_function(name).filter(|f| f.kinds().contains(&FunctionKind::Aggregate))
	}

	pub fn get_generator_function(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.get_function(name).filter(|f| f.kinds().contains(&FunctionKind::Generator))
	}

	pub fn function_names(&self) -> Vec<String> {
		self.functions.keys().map(|k| user_facing_name(k, BUILTIN_FUNCTION_PREFIX)).collect()
	}

	pub fn procedure_names(&self) -> Vec<String> {
		self.procedures.keys().map(|k| user_facing_name(k, BUILTIN_PROCEDURE_PREFIX)).collect()
	}

	pub fn scalar_function_names(&self) -> Vec<String> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Scalar))
			.map(|(k, _)| user_facing_name(k, BUILTIN_FUNCTION_PREFIX))
			.collect()
	}

	pub fn aggregate_function_names(&self) -> Vec<String> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Aggregate))
			.map(|(k, _)| user_facing_name(k, BUILTIN_FUNCTION_PREFIX))
			.collect()
	}

	pub fn generator_function_names(&self) -> Vec<String> {
		self.functions
			.iter()
			.filter(|(_, f)| f.kinds().contains(&FunctionKind::Generator))
			.map(|(k, _)| user_facing_name(k, BUILTIN_FUNCTION_PREFIX))
			.collect()
	}

	pub fn get_handlers(
		&self,
		catalog: &Catalog,
		txn: &mut Transaction<'_>,
		variant: VariantRef,
	) -> Vec<Arc<dyn Procedure>> {
		{
			let mut state = self.handlers.write().unwrap();
			if !state.deferred.is_empty() {
				let deferred = mem::take(&mut state.deferred);
				let mut still_deferred = Vec::new();
				for (path, handler) in deferred {
					match resolve_event_path(&path, catalog, txn) {
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
	functions: HashMap<String, Arc<dyn Function>>,
	procedures: HashMap<String, Arc<dyn Procedure>>,
	deferred_handlers: Vec<(String, Arc<dyn Procedure>)>,
}

impl RoutinesConfigurator {
	pub fn has_function(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedures.contains_key(name)
	}

	pub fn register_function(mut self, routine: Arc<dyn Function>) -> Self {
		self.functions.insert(routine.info().name.clone(), routine);
		self
	}

	pub fn register_procedure(mut self, routine: Arc<dyn Procedure>) -> Self {
		self.procedures.insert(routine.info().name.clone(), routine);
		self
	}

	pub fn register_builtin_function(mut self, routine: Arc<dyn Function>) -> Self {
		let key = format!("{BUILTIN_FUNCTION_PREFIX}{}", routine.info().name);
		self.functions.insert(key, routine);
		self
	}

	pub fn register_builtin_procedure(mut self, routine: Arc<dyn Procedure>) -> Self {
		let key = format!("{BUILTIN_PROCEDURE_PREFIX}{}", routine.info().name);
		self.procedures.insert(key, routine);
		self
	}

	pub fn register_handler(mut self, event_path: &str, routine: Arc<dyn Procedure>) -> Self {
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

	pub fn register_builtin_function_alias(mut self, alias: &str, canonical: &str) -> Self {
		let canonical_key = format!("{BUILTIN_FUNCTION_PREFIX}{canonical}");
		let alias_key = format!("{BUILTIN_FUNCTION_PREFIX}{alias}");
		if let Some(routine) = self.functions.get(&canonical_key).cloned() {
			self.functions.insert(alias_key, routine);
		}
		self
	}

	pub fn register_builtin_procedure_alias(mut self, alias: &str, canonical: &str) -> Self {
		let canonical_key = format!("{BUILTIN_PROCEDURE_PREFIX}{canonical}");
		let alias_key = format!("{BUILTIN_PROCEDURE_PREFIX}{alias}");
		if let Some(routine) = self.procedures.get(&canonical_key).cloned() {
			self.procedures.insert(alias_key, routine);
		}
		self
	}

	pub fn configure(self) -> Routines {
		let builtin_function_namespaces =
			collect_builtin_namespaces(self.functions.keys(), BUILTIN_FUNCTION_PREFIX);
		let builtin_procedure_namespaces =
			collect_builtin_namespaces(self.procedures.keys(), BUILTIN_PROCEDURE_PREFIX);
		Routines(Arc::new(RoutinesInner {
			functions: self.functions,
			procedures: self.procedures,
			builtin_function_namespaces,
			builtin_procedure_namespaces,
			handlers: RwLock::new(EventHandlerState {
				resolved: HashMap::new(),
				deferred: self.deferred_handlers,
			}),
		}))
	}
}

fn user_facing_name(key: &str, prefix: &str) -> String {
	key.strip_prefix(prefix).unwrap_or(key).to_string()
}

fn collect_builtin_namespaces<'a, I: Iterator<Item = &'a String>>(keys: I, prefix: &str) -> HashSet<String> {
	keys.filter_map(|k| k.strip_prefix(prefix))
		.filter_map(|tail| tail.split_once("::").map(|(ns, _)| ns.to_string()))
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		function::default_native_functions,
		procedure::{clock::set::ClockSetProcedure, default_native_procedures},
	};

	fn registry() -> Routines {
		default_native_procedures(default_native_functions(Routines::builder())).configure()
	}

	#[test]
	fn function_fallback_returns_same_arc_as_canonical() {
		let r = registry();
		let direct = r.get_function("math::abs").unwrap();
		let canonical = r.get_function("system::builtin::functions::math::abs").unwrap();
		assert!(Arc::ptr_eq(&direct, &canonical));
	}

	#[test]
	fn procedure_fallback_returns_same_arc_as_canonical() {
		let r = registry();
		let direct = r.get_procedure("clock::set").unwrap();
		let canonical = r.get_procedure("system::builtin::procedures::clock::set").unwrap();
		assert!(Arc::ptr_eq(&direct, &canonical));
	}

	#[test]
	fn procedure_multi_segment_fallback() {
		let r = registry();
		let direct = r.get_procedure("testing::events::dispatched").unwrap();
		let canonical = r.get_procedure("system::builtin::procedures::testing::events::dispatched").unwrap();
		assert!(Arc::ptr_eq(&direct, &canonical));
	}

	#[test]
	fn unknown_namespace_returns_none() {
		let r = registry();
		assert!(r.get_function("nonexistent::foo").is_none());
		assert!(r.get_procedure("nonexistent::bar").is_none());
	}

	#[test]
	fn unqualified_name_returns_none() {
		let r = registry();
		assert!(r.get_function("abs").is_none());
		assert!(r.get_procedure("set").is_none());
	}

	#[test]
	fn alias_returns_same_arc_as_canonical() {
		let r = registry();
		let day = r.get_function("duration::day").unwrap();
		let days = r.get_function("duration::days").unwrap();
		assert!(Arc::ptr_eq(&day, &days));
	}

	#[test]
	fn raw_registration_shadows_builtin() {
		let user_proc: Arc<dyn Procedure> = Arc::new(ClockSetProcedure::new());
		let r = default_native_procedures(Routines::builder())
			.register_procedure(user_proc.clone())
			.configure();
		let resolved = r.get_procedure("clock::set").unwrap();
		assert!(Arc::ptr_eq(&resolved, &user_proc));
	}

	#[test]
	fn name_listings_strip_canonical_prefix() {
		let r = registry();
		let function_names = r.function_names();
		assert!(function_names.iter().any(|n| n == "math::abs"));
		assert!(!function_names.iter().any(|n| n.starts_with("system::builtin::functions::")));
		let procedure_names = r.procedure_names();
		assert!(procedure_names.iter().any(|n| n == "clock::set"));
		assert!(!procedure_names.iter().any(|n| n.starts_with("system::builtin::procedures::")));
	}
}

fn resolve_event_path(path: &str, catalog: &Catalog, txn: &mut Transaction<'_>) -> Result<VariantRef, String> {
	let parts: Vec<&str> = path.split("::").collect();
	if parts.len() != 3 {
		return Err(format!(
			"Invalid event path '{}': expected format 'namespace::event_name::VariantName'",
			path
		));
	}
	let (namespace_name, event_name, variant_name) = (parts[0], parts[1], parts[2]);

	let namespace = catalog
		.find_namespace_by_name(txn, namespace_name)
		.map_err(|e| format!("find_namespace_by_name failed: {e}"))?
		.ok_or_else(|| format!("Namespace '{}' not found", namespace_name))?;

	let sumtype = catalog
		.find_sumtype_by_name(txn, namespace.id(), event_name)
		.map_err(|e| format!("find_sumtype_by_name failed: {e}"))?
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
