// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		binding::{Binding, BindingProtocol},
		id::{BindingId, NamespaceId, ProcedureId},
	},
};

use super::{CatalogCache, MultiVersionBinding};

impl CatalogCache {
	pub fn find_binding_at(&self, id: BindingId, version: CommitVersion) -> Option<Binding> {
		self.bindings.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_binding(&self, id: BindingId) -> Option<Binding> {
		self.bindings.get(&id).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_binding_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Binding> {
		self.bindings_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let id = *entry.value();
			self.find_binding_at(id, version)
		})
	}

	pub fn find_binding_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Binding> {
		self.bindings_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let id = *entry.value();
			self.find_binding(id)
		})
	}

	pub fn list_bindings_for_procedure_at(
		&self,
		procedure_id: ProcedureId,
		version: CommitVersion,
	) -> Vec<Binding> {
		if let Some(entry) = self.bindings_by_procedure.get(&procedure_id) {
			entry.value().iter().filter_map(|id| self.find_binding_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn find_grpc_binding_by_name(&self, name: &str) -> Option<Binding> {
		self.bindings_by_grpc_name.get(name).and_then(|entry| self.find_binding(*entry.value()))
	}

	pub fn find_grpc_binding_by_name_at(&self, name: &str, version: CommitVersion) -> Option<Binding> {
		self.bindings_by_grpc_name.get(name).and_then(|entry| self.find_binding_at(*entry.value(), version))
	}

	pub fn find_ws_binding_by_name(&self, name: &str) -> Option<Binding> {
		self.bindings_by_ws_name.get(name).and_then(|entry| self.find_binding(*entry.value()))
	}

	pub fn find_ws_binding_by_name_at(&self, name: &str, version: CommitVersion) -> Option<Binding> {
		self.bindings_by_ws_name.get(name).and_then(|entry| self.find_binding_at(*entry.value(), version))
	}

	pub fn find_http_binding_by_method_path(&self, method: &str, path: &str) -> Option<Binding> {
		self.bindings_by_http_method_path
			.get(&(method.to_string(), path.to_string()))
			.and_then(|entry| self.find_binding(*entry.value()))
	}

	pub fn find_http_binding_by_method_path_at(
		&self,
		method: &str,
		path: &str,
		version: CommitVersion,
	) -> Option<Binding> {
		self.bindings_by_http_method_path
			.get(&(method.to_string(), path.to_string()))
			.and_then(|entry| self.find_binding_at(*entry.value(), version))
	}

	pub fn list_http_bindings(&self) -> Vec<Binding> {
		self.bindings_http.iter().filter_map(|entry| self.find_binding(*entry.key())).collect()
	}

	pub fn list_http_bindings_at(&self, version: CommitVersion) -> Vec<Binding> {
		self.bindings_http.iter().filter_map(|entry| self.find_binding_at(*entry.key(), version)).collect()
	}

	pub fn set_binding(&self, id: BindingId, version: CommitVersion, binding: Option<Binding>) {
		if let Some(entry) = self.bindings.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			if let Some(ids_entry) = self.bindings_by_procedure.get(&pre.procedure_id) {
				let mut ids = ids_entry.value().clone();
				ids.retain(|existing| *existing != id);
				drop(ids_entry);
				if ids.is_empty() {
					self.bindings_by_procedure.remove(&pre.procedure_id);
				} else {
					self.bindings_by_procedure.insert(pre.procedure_id, ids);
				}
			}
			self.bindings_by_name.remove(&(pre.namespace, pre.name.clone()));
			match &pre.protocol {
				BindingProtocol::Grpc {
					name,
				} => {
					self.bindings_by_grpc_name.remove(name);
				}
				BindingProtocol::Ws {
					name,
				} => {
					self.bindings_by_ws_name.remove(name);
				}
				BindingProtocol::Http {
					method,
					path,
				} => {
					self.bindings_http.remove(&id);
					self.bindings_by_http_method_path
						.remove(&(method.as_str().to_string(), path.clone()));
				}
			}
		}

		let multi = self.bindings.get_or_insert_with(id, MultiVersionBinding::new);
		if let Some(new) = binding {
			if let Some(entry) = self.bindings_by_procedure.get(&new.procedure_id) {
				let mut ids = entry.value().clone();
				if !ids.contains(&id) {
					ids.push(id);
				}
				drop(entry);
				self.bindings_by_procedure.insert(new.procedure_id, ids);
			} else {
				self.bindings_by_procedure.insert(new.procedure_id, vec![id]);
			}

			self.bindings_by_name.insert((new.namespace, new.name.clone()), id);
			match &new.protocol {
				BindingProtocol::Grpc {
					name,
				} => {
					self.bindings_by_grpc_name.insert(name.clone(), id);
				}
				BindingProtocol::Ws {
					name,
				} => {
					self.bindings_by_ws_name.insert(name.clone(), id);
				}
				BindingProtocol::Http {
					method,
					path,
				} => {
					self.bindings_http.insert(id, ());
					self.bindings_by_http_method_path
						.insert((method.as_str().to_string(), path.clone()), id);
				}
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
