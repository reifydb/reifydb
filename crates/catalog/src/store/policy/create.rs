// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{SecurityPolicyDef, SecurityPolicyOperationDef, SecurityPolicyToCreate},
	key::{policy::SecurityPolicyKey, security_policy_op::SecurityPolicyOpKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::{
		policy::schema::{
			security_policy::{ENABLED, ID, NAME, SCHEMA, TARGET_NAMESPACE, TARGET_OBJECT, TARGET_TYPE},
			security_policy_op,
		},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_security_policy(
		txn: &mut AdminTransaction,
		to_create: SecurityPolicyToCreate,
	) -> crate::Result<(SecurityPolicyDef, Vec<SecurityPolicyOperationDef>)> {
		// Check duplicate by name if named
		if let Some(ref name) = to_create.name {
			if let Some(_) = Self::find_security_policy_by_name(&mut Transaction::Admin(&mut *txn), name)? {
				return Err(CatalogError::AlreadyExists {
					kind: CatalogObjectKind::SecurityPolicy,
					namespace: "system".to_string(),
					name: name.clone(),
					fragment: reifydb_type::fragment::Fragment::None,
				}
				.into());
			}
		}

		let policy_id = SystemSequence::next_security_policy_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, policy_id);
		SCHEMA.set_utf8(&mut row, NAME, to_create.name.as_deref().unwrap_or(""));
		SCHEMA.set_utf8(&mut row, TARGET_TYPE, to_create.target_type.as_str());
		SCHEMA.set_utf8(&mut row, TARGET_NAMESPACE, to_create.target_namespace.as_deref().unwrap_or(""));
		SCHEMA.set_utf8(&mut row, TARGET_OBJECT, to_create.target_object.as_deref().unwrap_or(""));
		SCHEMA.set_bool(&mut row, ENABLED, true);

		txn.set(&SecurityPolicyKey::encoded(policy_id), row)?;

		// Write operation rows
		let mut ops = Vec::new();
		for (i, op) in to_create.operations.iter().enumerate() {
			let mut op_row = security_policy_op::SCHEMA.allocate();
			security_policy_op::SCHEMA.set_u64(&mut op_row, security_policy_op::POLICY_ID, policy_id);
			security_policy_op::SCHEMA.set_utf8(&mut op_row, security_policy_op::OPERATION, &op.operation);
			security_policy_op::SCHEMA.set_utf8(
				&mut op_row,
				security_policy_op::BODY_SOURCE,
				&op.body_source,
			);

			txn.set(&SecurityPolicyOpKey::encoded(policy_id, i as u64), op_row)?;

			ops.push(SecurityPolicyOperationDef {
				policy_id,
				operation: op.operation.clone(),
				body_source: op.body_source.clone(),
			});
		}

		let def = SecurityPolicyDef {
			id: policy_id,
			name: to_create.name,
			target_type: to_create.target_type,
			target_namespace: to_create.target_namespace,
			target_object: to_create.target_object,
			enabled: true,
		};

		Ok((def, ops))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{
		PolicyTargetType, SecurityPolicyOpToCreate, SecurityPolicyToCreate,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_security_policy() {
		let mut txn = create_test_admin_transaction();
		let to_create = SecurityPolicyToCreate {
			name: Some("read_only".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![SecurityPolicyOpToCreate {
				operation: "SELECT".to_string(),
				body_source: "ALLOW".to_string(),
			}],
		};
		let (def, ops) = CatalogStore::create_security_policy(&mut txn, to_create).unwrap();
		assert_eq!(def.name, Some("read_only".to_string()));
		assert_eq!(def.target_type, PolicyTargetType::Table);
		assert!(def.enabled);
		assert_eq!(ops.len(), 1);
		assert_eq!(ops[0].operation, "SELECT");
	}

	#[test]
	fn test_create_security_policy_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_security_policy(
			&mut txn,
			SecurityPolicyToCreate {
				name: Some("read_only".to_string()),
				target_type: PolicyTargetType::Table,
				target_namespace: None,
				target_object: None,
				operations: vec![],
			},
		)
		.unwrap();
		let err = CatalogStore::create_security_policy(
			&mut txn,
			SecurityPolicyToCreate {
				name: Some("read_only".to_string()),
				target_type: PolicyTargetType::Table,
				target_namespace: None,
				target_object: None,
				operations: vec![],
			},
		)
		.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_042");
	}
}
