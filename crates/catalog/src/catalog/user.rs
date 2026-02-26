// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::{
		CatalogTrackRoleChangeOperations, CatalogTrackUserChangeOperations,
		CatalogTrackUserRoleChangeOperations,
	},
	user::{RoleDef, RoleId, UserDef, UserId, UserRoleDef},
};
use reifydb_transaction::{
	change::{TransactionalRoleChanges, TransactionalUserChanges},
	transaction::{Transaction, admin::AdminTransaction},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::user::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_user_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<Option<UserDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(user) = TransactionalUserChanges::find_user_by_name(admin, name) {
					return Ok(Some(user.clone()));
				}

				// 2. Check if deleted
				if TransactionalUserChanges::is_user_deleted_by_name(admin, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(user) = self.materialized.find_user_by_name_at(name, admin.version()) {
					return Ok(Some(user));
				}

				// 4. Fall back to storage
				if let Some(user) =
					CatalogStore::find_user_by_name(&mut Transaction::Admin(&mut *admin), name)?
				{
					warn!("User '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(user));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(user) = self.materialized.find_user_by_name_at(name, cmd.version()) {
					return Ok(Some(user));
				}

				if let Some(user) =
					CatalogStore::find_user_by_name(&mut Transaction::Command(&mut *cmd), name)?
				{
					warn!("User '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(user));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(user) = self.materialized.find_user_by_name_at(name, qry.version()) {
					return Ok(Some(user));
				}

				if let Some(user) =
					CatalogStore::find_user_by_name(&mut Transaction::Query(&mut *qry), name)?
				{
					warn!("User '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(user));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::user::create", level = "debug", skip(self, txn))]
	pub fn create_user(
		&self,
		txn: &mut AdminTransaction,
		name: &str,
		password_hash: &str,
	) -> crate::Result<UserDef> {
		let user = CatalogStore::create_user(txn, name, password_hash)?;
		txn.track_user_def_created(user.clone())?;
		Ok(user)
	}

	#[instrument(name = "catalog::user::drop", level = "debug", skip(self, txn))]
	pub fn drop_user(&self, txn: &mut AdminTransaction, user_id: UserId) -> crate::Result<()> {
		// Get the user def before dropping for change tracking
		if let Some(user) = CatalogStore::find_user(&mut Transaction::Admin(&mut *txn), user_id)? {
			CatalogStore::drop_user(txn, user_id)?;
			txn.track_user_def_deleted(user)?;
		} else {
			CatalogStore::drop_user(txn, user_id)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::role::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_role_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> crate::Result<Option<RoleDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				if let Some(role) = TransactionalRoleChanges::find_role_by_name(admin, name) {
					return Ok(Some(role.clone()));
				}

				if TransactionalRoleChanges::is_role_deleted_by_name(admin, name) {
					return Ok(None);
				}

				if let Some(role) = self.materialized.find_role_by_name_at(name, admin.version()) {
					return Ok(Some(role));
				}

				if let Some(role) =
					CatalogStore::find_role_by_name(&mut Transaction::Admin(&mut *admin), name)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(role));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(role) = self.materialized.find_role_by_name_at(name, cmd.version()) {
					return Ok(Some(role));
				}

				if let Some(role) =
					CatalogStore::find_role_by_name(&mut Transaction::Command(&mut *cmd), name)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(role));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(role) = self.materialized.find_role_by_name_at(name, qry.version()) {
					return Ok(Some(role));
				}

				if let Some(role) =
					CatalogStore::find_role_by_name(&mut Transaction::Query(&mut *qry), name)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(role));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::role::create", level = "debug", skip(self, txn))]
	pub fn create_role(&self, txn: &mut AdminTransaction, name: &str) -> crate::Result<RoleDef> {
		let role = CatalogStore::create_role(txn, name)?;
		txn.track_role_def_created(role.clone())?;
		Ok(role)
	}

	#[instrument(name = "catalog::role::drop", level = "debug", skip(self, txn))]
	pub fn drop_role(&self, txn: &mut AdminTransaction, role_id: RoleId) -> crate::Result<()> {
		if let Some(role) = CatalogStore::find_role(&mut Transaction::Admin(&mut *txn), role_id)? {
			CatalogStore::drop_role(txn, role_id)?;
			txn.track_role_def_deleted(role)?;
		} else {
			CatalogStore::drop_role(txn, role_id)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::user::grant_role", level = "debug", skip(self, txn))]
	pub fn grant_role(
		&self,
		txn: &mut AdminTransaction,
		user_id: UserId,
		role_id: RoleId,
	) -> crate::Result<UserRoleDef> {
		let ur = CatalogStore::grant_role(txn, user_id, role_id)?;
		txn.track_user_role_def_created(ur.clone())?;
		Ok(ur)
	}

	#[instrument(name = "catalog::user::revoke_role", level = "debug", skip(self, txn))]
	pub fn revoke_role(&self, txn: &mut AdminTransaction, user_id: UserId, role_id: RoleId) -> crate::Result<()> {
		let ur = UserRoleDef {
			user_id,
			role_id,
		};
		CatalogStore::revoke_role(txn, user_id, role_id)?;
		txn.track_user_role_def_deleted(ur)?;
		Ok(())
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
