// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::authentication_method::{AuthenticationMethod, AuthenticationMethodId},
};

use crate::cache::{CatalogCache, MultiVersionAuthenticationMethod};

impl CatalogCache {
	pub fn list_all_authentication_methods_at(&self, version: CommitVersion) -> Vec<AuthenticationMethod> {
		self.authentication_methods.iter().filter_map(|entry| entry.value().get(version)).collect()
	}

	pub fn find_authentication_method_at(
		&self,
		id: AuthenticationMethodId,
		version: CommitVersion,
	) -> Option<AuthenticationMethod> {
		self.authentication_methods.get(&id).and_then(|entry| entry.value().get(version))
	}

	pub fn find_authentication_method_by_name_at(
		&self,
		name: &str,
		version: CommitVersion,
	) -> Option<AuthenticationMethod> {
		self.authentication_methods_by_name
			.get(name)
			.and_then(|entry| self.find_authentication_method_at(*entry.value(), version))
	}

	pub fn set_authentication_method(
		&self,
		id: AuthenticationMethodId,
		version: CommitVersion,
		method: Option<AuthenticationMethod>,
	) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.authentication_methods.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.authentication_methods_by_name.remove(&pre.name);
		}

		let multi = self.authentication_methods.get_or_insert_with(id, MultiVersionAuthenticationMethod::new);
		if let Some(new) = method {
			self.authentication_methods_by_name.insert(new.name.clone(), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
