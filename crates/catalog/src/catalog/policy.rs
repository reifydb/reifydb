// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSecurityPolicyChangeOperations,
	policy::{SecurityPolicyDef, SecurityPolicyId, SecurityPolicyOperationDef, SecurityPolicyToCreate},
};
use reifydb_transaction::{
	change::TransactionalSecurityPolicyChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use tracing::{instrument, warn};

use crate::{
	CatalogStore,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::security_policy::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_security_policy_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: &str,
	) -> crate::Result<Option<SecurityPolicyDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(policy) =
					TransactionalSecurityPolicyChanges::find_security_policy_by_name(admin, name)
				{
					return Ok(Some(policy.clone()));
				}

				// 2. Check if deleted
				if TransactionalSecurityPolicyChanges::is_security_policy_deleted_by_name(admin, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(policy) =
					self.materialized.find_security_policy_by_name_at(name, admin.version())
				{
					return Ok(Some(policy));
				}

				// 4. Fall back to storage
				if let Some(policy) = CatalogStore::find_security_policy_by_name(
					&mut Transaction::Admin(&mut *admin),
					name,
				)? {
					warn!(
						"SecurityPolicy '{}' found in storage but not in MaterializedCatalog",
						name
					);
					return Ok(Some(policy));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(policy) =
					self.materialized.find_security_policy_by_name_at(name, cmd.version())
				{
					return Ok(Some(policy));
				}

				if let Some(policy) = CatalogStore::find_security_policy_by_name(
					&mut Transaction::Command(&mut *cmd),
					name,
				)? {
					warn!(
						"SecurityPolicy '{}' found in storage but not in MaterializedCatalog",
						name
					);
					return Ok(Some(policy));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(policy) =
					self.materialized.find_security_policy_by_name_at(name, qry.version())
				{
					return Ok(Some(policy));
				}

				if let Some(policy) = CatalogStore::find_security_policy_by_name(
					&mut Transaction::Query(&mut *qry),
					name,
				)? {
					warn!(
						"SecurityPolicy '{}' found in storage but not in MaterializedCatalog",
						name
					);
					return Ok(Some(policy));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::security_policy::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_security_policy(
		&self,
		txn: &mut AdminTransaction,
		to_create: SecurityPolicyToCreate,
	) -> crate::Result<(SecurityPolicyDef, Vec<SecurityPolicyOperationDef>)> {
		let (policy, ops) = CatalogStore::create_security_policy(txn, to_create)?;
		txn.track_security_policy_def_created(policy.clone())?;
		Ok((policy, ops))
	}

	#[instrument(name = "catalog::security_policy::alter", level = "debug", skip(self, txn))]
	pub fn alter_security_policy(
		&self,
		txn: &mut AdminTransaction,
		policy_id: SecurityPolicyId,
		enabled: bool,
	) -> crate::Result<()> {
		// Read pre-state
		let pre = CatalogStore::find_security_policy(&mut Transaction::Admin(&mut *txn), policy_id)?;

		CatalogStore::alter_security_policy_enabled(txn, policy_id, enabled)?;

		// Read post-state
		let post = CatalogStore::find_security_policy(&mut Transaction::Admin(&mut *txn), policy_id)?;

		if let (Some(pre), Some(post)) = (pre, post) {
			txn.track_security_policy_def_updated(pre, post)?;
		}

		Ok(())
	}

	#[instrument(name = "catalog::security_policy::drop", level = "debug", skip(self, txn))]
	pub fn drop_security_policy(
		&self,
		txn: &mut AdminTransaction,
		policy_id: SecurityPolicyId,
	) -> crate::Result<()> {
		// Get the policy def before dropping for change tracking
		if let Some(policy) = CatalogStore::find_security_policy(&mut Transaction::Admin(&mut *txn), policy_id)?
		{
			CatalogStore::drop_security_policy(txn, policy_id)?;
			txn.track_security_policy_def_deleted(policy)?;
		} else {
			CatalogStore::drop_security_policy(txn, policy_id)?;
		}
		Ok(())
	}

	pub fn get_security_policy_by_name(
		&self,
		txn: &mut Transaction<'_>,
		name: &str,
	) -> crate::Result<SecurityPolicyDef> {
		self.find_security_policy_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::SecurityPolicy,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: reifydb_type::fragment::Fragment::None,
			}
			.into()
		})
	}

	pub fn list_all_security_policies(&self, txn: &mut Transaction<'_>) -> crate::Result<Vec<SecurityPolicyDef>> {
		CatalogStore::list_all_security_policies(txn)
	}

	pub fn list_security_policy_operations(
		&self,
		txn: &mut Transaction<'_>,
		policy_id: SecurityPolicyId,
	) -> crate::Result<Vec<SecurityPolicyOperationDef>> {
		CatalogStore::list_security_policy_operations(txn, policy_id)
	}
}
