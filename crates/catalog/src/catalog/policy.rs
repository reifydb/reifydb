// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::security_policy::{
	SecurityPolicyDef, SecurityPolicyId, SecurityPolicyOperationDef, SecurityPolicyToCreate,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use tracing::instrument;

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
		CatalogStore::find_security_policy_by_name(txn, name)
	}

	#[instrument(name = "catalog::security_policy::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_security_policy(
		&self,
		txn: &mut AdminTransaction,
		to_create: SecurityPolicyToCreate,
	) -> crate::Result<(SecurityPolicyDef, Vec<SecurityPolicyOperationDef>)> {
		CatalogStore::create_security_policy(txn, to_create)
	}

	#[instrument(name = "catalog::security_policy::alter", level = "debug", skip(self, txn))]
	pub fn alter_security_policy(
		&self,
		txn: &mut AdminTransaction,
		policy_id: SecurityPolicyId,
		enabled: bool,
	) -> crate::Result<()> {
		CatalogStore::alter_security_policy_enabled(txn, policy_id, enabled)
	}

	#[instrument(name = "catalog::security_policy::drop", level = "debug", skip(self, txn))]
	pub fn drop_security_policy(
		&self,
		txn: &mut AdminTransaction,
		policy_id: SecurityPolicyId,
	) -> crate::Result<()> {
		CatalogStore::drop_security_policy(txn, policy_id)
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
}
