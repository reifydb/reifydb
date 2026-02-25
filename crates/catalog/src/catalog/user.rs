// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::user::{RoleDef, RoleId, UserDef, UserId, UserRoleDef};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

use crate::{
	CatalogStore,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::user::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_user_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<Option<UserDef>> {
		CatalogStore::find_user_by_name(txn, name)
	}

	#[instrument(name = "catalog::user::create", level = "debug", skip(self, txn))]
	pub fn create_user(
		&self,
		txn: &mut AdminTransaction,
		name: &str,
		password_hash: &str,
	) -> crate::Result<UserDef> {
		CatalogStore::create_user(txn, name, password_hash)
	}

	#[instrument(name = "catalog::user::drop", level = "debug", skip(self, txn))]
	pub fn drop_user(&self, txn: &mut AdminTransaction, user_id: UserId) -> crate::Result<()> {
		CatalogStore::drop_user(txn, user_id)
	}

	#[instrument(name = "catalog::role::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_role_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<Option<RoleDef>> {
		CatalogStore::find_role_by_name(txn, name)
	}

	#[instrument(name = "catalog::role::create", level = "debug", skip(self, txn))]
	pub fn create_role(&self, txn: &mut AdminTransaction, name: &str) -> crate::Result<RoleDef> {
		CatalogStore::create_role(txn, name)
	}

	#[instrument(name = "catalog::role::drop", level = "debug", skip(self, txn))]
	pub fn drop_role(&self, txn: &mut AdminTransaction, role_id: RoleId) -> crate::Result<()> {
		CatalogStore::drop_role(txn, role_id)
	}

	#[instrument(name = "catalog::user::grant_role", level = "debug", skip(self, txn))]
	pub fn grant_role(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		role_id: RoleId,
	) -> crate::Result<UserRoleDef> {
		CatalogStore::grant_role(txn, user_id, role_id)
	}

	#[instrument(name = "catalog::user::revoke_role", level = "debug", skip(self, txn))]
	pub fn revoke_role(&self, txn: &mut AdminTransaction, user_id: UserId, role_id: RoleId) -> crate::Result<()> {
		CatalogStore::revoke_role(txn, user_id, role_id)
	}

	pub fn get_user_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<UserDef> {
		self.find_user_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::User,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: reifydb_type::fragment::Fragment::None,
			}
			.into()
		})
	}

	pub fn get_role_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<RoleDef> {
		self.find_role_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Role,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: reifydb_type::fragment::Fragment::None,
			}
			.into()
		})
	}
}
