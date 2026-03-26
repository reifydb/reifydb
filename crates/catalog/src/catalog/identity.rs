// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::{
		CatalogTrackIdentityChangeOperations, CatalogTrackIdentityRoleChangeOperations,
		CatalogTrackRoleChangeOperations,
	},
	identity::{IdentityDef, IdentityRoleDef, RoleDef, RoleId},
};
use reifydb_transaction::{
	change::{TransactionalIdentityChanges, TransactionalIdentityRoleChanges, TransactionalRoleChanges},
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
	#[instrument(name = "catalog::identity::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_identity_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<IdentityDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(ident) = TransactionalIdentityChanges::find_identity_by_name(admin, name) {
					return Ok(Some(ident.clone()));
				}

				// 2. Check if deleted
				if TransactionalIdentityChanges::is_identity_deleted_by_name(admin, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(ident) = self.materialized.find_identity_by_name_at(name, admin.version()) {
					return Ok(Some(ident));
				}

				// 4. Fall back to storage
				if let Some(ident) =
					CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut *admin), name)?
				{
					warn!("Identity '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(ident) = self.materialized.find_identity_by_name_at(name, cmd.version()) {
					return Ok(Some(ident));
				}

				if let Some(ident) =
					CatalogStore::find_identity_by_name(&mut Transaction::Command(&mut *cmd), name)?
				{
					warn!("Identity '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(ident) = self.materialized.find_identity_by_name_at(name, qry.version()) {
					return Ok(Some(ident));
				}

				if let Some(ident) =
					CatalogStore::find_identity_by_name(&mut Transaction::Query(&mut *qry), name)?
				{
					warn!("Identity '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Subscription(sub) => {
				// 1. Check transactional changes first
				if let Some(ident) = TransactionalIdentityChanges::find_identity_by_name(sub, name) {
					return Ok(Some(ident.clone()));
				}

				// 2. Check if deleted
				if TransactionalIdentityChanges::is_identity_deleted_by_name(sub, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(ident) = self.materialized.find_identity_by_name_at(name, sub.version()) {
					return Ok(Some(ident));
				}

				// 4. Fall back to storage
				if let Some(ident) = CatalogStore::find_identity_by_name(
					&mut Transaction::Subscription(&mut *sub),
					name,
				)? {
					warn!("Identity '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(ident));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::identity::find", level = "trace", skip(self, txn))]
	pub fn find_identity(&self, txn: &mut Transaction<'_>, identity: IdentityId) -> Result<Option<IdentityDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(ident) = TransactionalIdentityChanges::find_identity(admin, identity) {
					return Ok(Some(ident.clone()));
				}

				// 2. Check if deleted
				if TransactionalIdentityChanges::is_identity_deleted(admin, identity) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(ident) = self.materialized.find_identity_at(identity, admin.version()) {
					return Ok(Some(ident));
				}

				// 4. Fall back to storage
				if let Some(ident) =
					CatalogStore::find_identity(&mut Transaction::Admin(&mut *admin), identity)?
				{
					warn!(
						"Identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(ident) = self.materialized.find_identity_at(identity, cmd.version()) {
					return Ok(Some(ident));
				}

				if let Some(ident) =
					CatalogStore::find_identity(&mut Transaction::Command(&mut *cmd), identity)?
				{
					warn!(
						"Identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(ident) = self.materialized.find_identity_at(identity, qry.version()) {
					return Ok(Some(ident));
				}

				if let Some(ident) =
					CatalogStore::find_identity(&mut Transaction::Query(&mut *qry), identity)?
				{
					warn!(
						"Identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(ident));
				}

				Ok(None)
			}
			Transaction::Subscription(sub) => {
				// 1. Check transactional changes first
				if let Some(ident) = TransactionalIdentityChanges::find_identity(sub, identity) {
					return Ok(Some(ident.clone()));
				}

				// 2. Check if deleted
				if TransactionalIdentityChanges::is_identity_deleted(sub, identity) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(ident) = self.materialized.find_identity_at(identity, sub.version()) {
					return Ok(Some(ident));
				}

				// 4. Fall back to storage
				if let Some(ident) = CatalogStore::find_identity(
					&mut Transaction::Subscription(&mut *sub),
					identity,
				)? {
					warn!(
						"Identity '{}' found in storage but not in MaterializedCatalog",
						identity
					);
					return Ok(Some(ident));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::identity::create", level = "debug", skip(self, txn))]
	pub fn create_identity(&self, txn: &mut AdminTransaction, name: &str) -> Result<IdentityDef> {
		let ident = CatalogStore::create_identity(txn, name)?;
		txn.track_identity_def_created(ident.clone())?;
		Ok(ident)
	}

	#[instrument(name = "catalog::identity::drop", level = "debug", skip(self, txn))]
	pub fn drop_identity(&self, txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		// Get the identity def before dropping for change tracking
		if let Some(ident) = CatalogStore::find_identity(&mut Transaction::Admin(&mut *txn), identity)? {
			CatalogStore::drop_identity(txn, identity)?;
			txn.track_identity_def_deleted(ident)?;
		} else {
			CatalogStore::drop_identity(txn, identity)?;
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
			Transaction::Subscription(sub) => {
				if let Some(role) = TransactionalRoleChanges::find_role_by_name(sub, name) {
					return Ok(Some(role.clone()));
				}

				if TransactionalRoleChanges::is_role_deleted_by_name(sub, name) {
					return Ok(None);
				}

				if let Some(role) = self.materialized.find_role_by_name_at(name, sub.version()) {
					return Ok(Some(role));
				}

				if let Some(role) = CatalogStore::find_role_by_name(
					&mut Transaction::Subscription(&mut *sub),
					name,
				)? {
					warn!("Role '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(role));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::role::find", level = "trace", skip(self, txn))]
	pub fn find_role(&self, txn: &mut Transaction<'_>, role_id: RoleId) -> Result<Option<RoleDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(role) = TransactionalRoleChanges::find_role(admin, role_id) {
					return Ok(Some(role.clone()));
				}

				// 2. Check if deleted
				if TransactionalRoleChanges::is_role_deleted(admin, role_id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(role) = self.materialized.find_role_at(role_id, admin.version()) {
					return Ok(Some(role));
				}

				// 4. Fall back to storage
				if let Some(role) =
					CatalogStore::find_role(&mut Transaction::Admin(&mut *admin), role_id)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", role_id);
					return Ok(Some(role));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(role) = self.materialized.find_role_at(role_id, cmd.version()) {
					return Ok(Some(role));
				}

				if let Some(role) =
					CatalogStore::find_role(&mut Transaction::Command(&mut *cmd), role_id)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", role_id);
					return Ok(Some(role));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(role) = self.materialized.find_role_at(role_id, qry.version()) {
					return Ok(Some(role));
				}

				if let Some(role) =
					CatalogStore::find_role(&mut Transaction::Query(&mut *qry), role_id)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", role_id);
					return Ok(Some(role));
				}

				Ok(None)
			}
			Transaction::Subscription(sub) => {
				// 1. Check transactional changes first
				if let Some(role) = TransactionalRoleChanges::find_role(sub, role_id) {
					return Ok(Some(role.clone()));
				}

				// 2. Check if deleted
				if TransactionalRoleChanges::is_role_deleted(sub, role_id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(role) = self.materialized.find_role_at(role_id, sub.version()) {
					return Ok(Some(role));
				}

				// 4. Fall back to storage
				if let Some(role) =
					CatalogStore::find_role(&mut Transaction::Subscription(&mut *sub), role_id)?
				{
					warn!("Role '{}' found in storage but not in MaterializedCatalog", role_id);
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

	#[instrument(name = "catalog::identity::grant_role", level = "debug", skip(self, txn))]
	pub fn grant_role(
		&self,
		txn: &mut AdminTransaction,
		identity: IdentityId,
		role_id: RoleId,
	) -> Result<IdentityRoleDef> {
		let ir = CatalogStore::grant_role(txn, identity, role_id)?;
		txn.track_identity_role_def_created(ir.clone())?;
		Ok(ir)
	}

	#[instrument(name = "catalog::identity::revoke_role", level = "debug", skip(self, txn))]
	pub fn revoke_role(&self, txn: &mut AdminTransaction, identity: IdentityId, role_id: RoleId) -> Result<()> {
		let ir = IdentityRoleDef {
			identity,
			role_id,
		};
		CatalogStore::revoke_role(txn, identity, role_id)?;
		txn.track_identity_role_def_deleted(ir)?;
		Ok(())
	}

	#[instrument(name = "catalog::identity::find_role_names_for_identity", level = "trace", skip(self, txn))]
	pub fn find_role_names_for_identity(
		&self,
		txn: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<String>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				let version = admin.version();
				let mut names = Vec::new();
				let mut seen_roles = std::collections::HashSet::new();

				// 1. Check transactional identity-role changes first
				for ir in TransactionalIdentityRoleChanges::find_identity_roles_for_identity(
					admin, identity,
				) {
					if !TransactionalRoleChanges::is_role_deleted(admin, ir.role_id) {
						if let Some(role) =
							TransactionalRoleChanges::find_role(admin, ir.role_id)
						{
							seen_roles.insert(ir.role_id);
							names.push(role.name.clone());
						} else if let Some(role) =
							self.materialized.find_role_at(ir.role_id, version)
						{
							seen_roles.insert(ir.role_id);
							names.push(role.name);
						}
					}
				}

				// 2. Check materialized identity-roles
				for ir in self.materialized.find_identity_roles_at(identity, version) {
					if !seen_roles.contains(&ir.role_id)
						&& !TransactionalIdentityRoleChanges::is_identity_role_deleted(
							admin, identity, ir.role_id,
						) {
						if let Some(role) = self.materialized.find_role_at(ir.role_id, version)
						{
							names.push(role.name);
						}
					}
				}

				Ok(names)
			}
			Transaction::Subscription(sub) => {
				let version = sub.version();
				let mut names = Vec::new();
				let mut seen_roles = std::collections::HashSet::new();

				// 1. Check transactional identity-role changes first
				for ir in TransactionalIdentityRoleChanges::find_identity_roles_for_identity(
					sub, identity,
				) {
					if !TransactionalRoleChanges::is_role_deleted(sub, ir.role_id) {
						if let Some(role) = TransactionalRoleChanges::find_role(sub, ir.role_id)
						{
							seen_roles.insert(ir.role_id);
							names.push(role.name.clone());
						} else if let Some(role) =
							self.materialized.find_role_at(ir.role_id, version)
						{
							seen_roles.insert(ir.role_id);
							names.push(role.name);
						}
					}
				}

				// 2. Check materialized identity-roles
				for ir in self.materialized.find_identity_roles_at(identity, version) {
					if !seen_roles.contains(&ir.role_id)
						&& !TransactionalIdentityRoleChanges::is_identity_role_deleted(
							sub, identity, ir.role_id,
						) {
						if let Some(role) = self.materialized.find_role_at(ir.role_id, version)
						{
							names.push(role.name);
						}
					}
				}

				Ok(names)
			}
			_ => {
				let version = match txn.reborrow() {
					Transaction::Command(cmd) => cmd.version(),
					Transaction::Query(qry) => qry.version(),
					_ => unreachable!(),
				};

				let identity_roles = self.materialized.find_identity_roles_at(identity, version);
				let mut names = Vec::with_capacity(identity_roles.len());
				for ir in identity_roles {
					if let Some(role) = self.materialized.find_role_at(ir.role_id, version) {
						names.push(role.name);
					}
				}
				Ok(names)
			}
		}
	}

	pub fn get_identity_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<IdentityDef> {
		self.find_identity_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Identity,
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
