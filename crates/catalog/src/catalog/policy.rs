// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackPolicyChangeOperations,
	policy::{PolicyDef, PolicyId, PolicyOperationDef, PolicyToCreate},
};
use reifydb_transaction::{
	change::TransactionalPolicyChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};

impl Catalog {
	#[instrument(name = "catalog::policy::find_by_name", level = "trace", skip(self, txn))]
	pub fn find_policy_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<PolicyDef>> {
		match txn.reborrow() {
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(policy) = TransactionalPolicyChanges::find_policy_by_name(admin, name) {
					return Ok(Some(policy.clone()));
				}

				// 2. Check if deleted
				if TransactionalPolicyChanges::is_policy_deleted_by_name(admin, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(policy) = self.materialized.find_policy_by_name_at(name, admin.version()) {
					return Ok(Some(policy));
				}

				// 4. Fall back to storage
				if let Some(policy) =
					CatalogStore::find_policy_by_name(&mut Transaction::Admin(&mut *admin), name)?
				{
					warn!("Policy '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(policy));
				}

				Ok(None)
			}
			Transaction::Command(cmd) => {
				if let Some(policy) = self.materialized.find_policy_by_name_at(name, cmd.version()) {
					return Ok(Some(policy));
				}

				if let Some(policy) =
					CatalogStore::find_policy_by_name(&mut Transaction::Command(&mut *cmd), name)?
				{
					warn!("Policy '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(policy));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(policy) = self.materialized.find_policy_by_name_at(name, qry.version()) {
					return Ok(Some(policy));
				}

				if let Some(policy) =
					CatalogStore::find_policy_by_name(&mut Transaction::Query(&mut *qry), name)?
				{
					warn!("Policy '{}' found in storage but not in MaterializedCatalog", name);
					return Ok(Some(policy));
				}

				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::policy::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_policy(
		&self,
		txn: &mut AdminTransaction,
		to_create: PolicyToCreate,
	) -> Result<(PolicyDef, Vec<PolicyOperationDef>)> {
		let (policy, ops) = CatalogStore::create_policy(txn, to_create)?;
		txn.track_policy_def_created(policy.clone())?;
		Ok((policy, ops))
	}

	#[instrument(name = "catalog::policy::alter", level = "debug", skip(self, txn))]
	pub fn alter_policy(&self, txn: &mut AdminTransaction, policy_id: PolicyId, enabled: bool) -> Result<()> {
		// Read pre-state
		let pre = CatalogStore::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)?;

		CatalogStore::alter_policy_enabled(txn, policy_id, enabled)?;

		// Read post-state
		let post = CatalogStore::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)?;

		if let (Some(pre), Some(post)) = (pre, post) {
			txn.track_policy_def_updated(pre, post)?;
		}

		Ok(())
	}

	#[instrument(name = "catalog::policy::drop", level = "debug", skip(self, txn))]
	pub fn drop_policy(&self, txn: &mut AdminTransaction, policy_id: PolicyId) -> Result<()> {
		// Get the policy def before dropping for change tracking
		if let Some(policy) = CatalogStore::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)? {
			CatalogStore::drop_policy(txn, policy_id)?;
			txn.track_policy_def_deleted(policy)?;
		} else {
			CatalogStore::drop_policy(txn, policy_id)?;
		}
		Ok(())
	}

	pub fn get_policy_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<PolicyDef> {
		self.find_policy_by_name(txn, name)?.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::Policy,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})
	}

	pub fn list_all_policies(&self, txn: &mut Transaction<'_>) -> Result<Vec<PolicyDef>> {
		CatalogStore::list_all_policies(txn)
	}

	pub fn list_policy_operations(
		&self,
		txn: &mut Transaction<'_>,
		policy_id: PolicyId,
	) -> Result<Vec<PolicyOperationDef>> {
		CatalogStore::list_policy_operations(txn, policy_id)
	}
}
