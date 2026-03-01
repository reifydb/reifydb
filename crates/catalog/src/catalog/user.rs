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
use reifydb_type::{fragment::Fragment, value::identity::IdentityId};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::user::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_user_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<UserDef>> {
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

	#[instrument(name = "catalog::user::find_by_identity", level = "trace", skip(self, txn))]
	pub fn find_user_by_identity(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Option<UserDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				if let Some(user) =
					self.materialized.find_user_by_identity_at(identity, admin.version())
				{
					return Ok(Some(user));
				}

				if let Some(user) = CatalogStore::find_user_by_identity(
					&mut Transaction::Admin(&mut *admin),
					identity,
				)? {
					warn!(
						"User with identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(user));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(user) = self.materialized.find_user_by_identity_at(identity, cmd.version())
				{
					return Ok(Some(user));
				}

				if let Some(user) = CatalogStore::find_user_by_identity(
					&mut Transaction::Command(&mut *cmd),
					identity,
				)? {
					warn!(
						"User with identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(user));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(user) = self.materialized.find_user_by_identity_at(identity, qry.version())
				{
					return Ok(Some(user));
				}

				if let Some(user) = CatalogStore::find_user_by_identity(
					&mut Transaction::Query(&mut *qry),
					identity,
				)? {
					warn!(
						"User with identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(user));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::user::create", level = "debug", skip(self, txn))]
	pub fn create_user(&self, txn: &mut AdminTransaction, name: &str) -> Result<UserDef> {
		let user = CatalogStore::create_user(txn, name)?;
		txn.track_user_def_created(user.clone())?;
		Ok(user)
	}

	#[instrument(name = "catalog::user::drop", level = "debug", skip(self, txn))]
	pub fn drop_user(&self, txn: &mut AdminTransaction, user_id: UserId) -> Result<()> {
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
	pub fn find_role_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<RoleDef>> {
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
	pub fn create_role(&self, txn: &mut AdminTransaction, name: &str) -> Result<RoleDef> {
		let role = CatalogStore::create_role(txn, name)?;
		txn.track_role_def_created(role.clone())?;
		Ok(role)
	}

	#[instrument(name = "catalog::role::drop", level = "debug", skip(self, txn))]
	pub fn drop_role(&self, txn: &mut AdminTransaction, role_id: RoleId) -> Result<()> {
		if let Some(role) = CatalogStore::find_role(&mut Transaction::Admin(&mut *txn), role_id)? {
			CatalogStore::drop_role(txn, role_id)?;
			txn.track_role_def_deleted(role)?;
		} else {
			CatalogStore::drop_role(txn, role_id)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::user::grant_role", level = "debug", skip(self, txn))]
	pub fn grant_role(&self, txn: &mut AdminTransaction, user_id: UserId, role_id: RoleId) -> Result<UserRoleDef> {
		let ur = CatalogStore::grant_role(txn, user_id, role_id)?;
		txn.track_user_role_def_created(ur.clone())?;
		Ok(ur)
	}

	#[instrument(name = "catalog::user::revoke_role", level = "debug", skip(self, txn))]
	pub fn revoke_role(&self, txn: &mut AdminTransaction, user_id: UserId, role_id: RoleId) -> Result<()> {
		let ur = UserRoleDef {
			user_id,
			role_id,
		};
		CatalogStore::revoke_role(txn, user_id, role_id)?;
		txn.track_user_role_def_deleted(ur)?;
		Ok(())
	}

	#[instrument(name = "catalog::user::find_role_names_for_identity", level = "trace", skip(self, txn))]
	pub fn find_role_names_for_identity(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<String>> {
		let user = match self.find_user_by_identity(txn, identity)? {
			Some(u) => u,
			None => return Ok(vec![]),
		};

		let version = match txn.reborrow() {
			Transaction::Admin(admin) => admin.version(),
			Transaction::Command(cmd) => cmd.version(),
			Transaction::Query(qry) => qry.version(),
		};

		let user_roles = self.materialized.find_user_roles_for_user_at(user.id, version);
		let mut names = Vec::with_capacity(user_roles.len());
		for ur in user_roles {
			if let Some(role) = self.materialized.find_role_at(ur.role_id, version) {
				names.push(role.name);
			}
		}
		Ok(names)
	}

	pub fn get_user_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<UserDef> {
		self.find_user_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::User,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})
	}

	pub fn get_role_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<RoleDef> {
		self.find_role_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Role,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})
	}
}
