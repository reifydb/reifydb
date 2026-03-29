// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{Policy, PolicyOperation, PolicyToCreate},
	key::{policy::PolicyKey, policy_op::PolicyOpKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		policy::shape::{
			policy::{ENABLED, ID, NAME, SHAPE, TARGET_NAMESPACE, TARGET_OBJECT, TARGET_TYPE},
			policy_op,
		},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_policy(
		txn: &mut AdminTransaction,
		to_create: PolicyToCreate,
	) -> Result<(Policy, Vec<PolicyOperation>)> {
		// Check duplicate by name if named
		if let Some(ref name) = to_create.name {
			if let Some(_) = Self::find_policy_by_name(&mut Transaction::Admin(&mut *txn), name)? {
				return Err(CatalogError::AlreadyExists {
					kind: CatalogObjectKind::Policy,
					namespace: "system".to_string(),
					name: name.clone(),
					fragment: Fragment::None,
				}
				.into());
			}
		}

		let policy_id = SystemSequence::next_policy_id(txn)?;

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, policy_id);
		SHAPE.set_utf8(&mut row, NAME, to_create.name.as_deref().unwrap_or(""));
		SHAPE.set_utf8(&mut row, TARGET_TYPE, to_create.target_type.as_str());
		SHAPE.set_utf8(&mut row, TARGET_NAMESPACE, to_create.target_namespace.as_deref().unwrap_or(""));
		SHAPE.set_utf8(&mut row, TARGET_OBJECT, to_create.target_object.as_deref().unwrap_or(""));
		SHAPE.set_bool(&mut row, ENABLED, true);

		txn.set(&PolicyKey::encoded(policy_id), row)?;

		// Write operation rows
		let mut ops = Vec::new();
		for (i, op) in to_create.operations.iter().enumerate() {
			let mut op_row = policy_op::SHAPE.allocate();
			policy_op::SHAPE.set_u64(&mut op_row, policy_op::POLICY_ID, policy_id);
			policy_op::SHAPE.set_utf8(&mut op_row, policy_op::OPERATION, &op.operation);
			policy_op::SHAPE.set_utf8(&mut op_row, policy_op::BODY_SOURCE, &op.body_source);

			txn.set(&PolicyOpKey::encoded(policy_id, i as u64), op_row)?;

			ops.push(PolicyOperation {
				policy_id,
				operation: op.operation.clone(),
				body_source: op.body_source.clone(),
			});
		}

		let def = Policy {
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
	use reifydb_core::interface::catalog::policy::{PolicyOpToCreate, PolicyTargetType, PolicyToCreate};
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_policy() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("read_only".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![PolicyOpToCreate {
				operation: "SELECT".to_string(),
				body_source: "ALLOW".to_string(),
			}],
		};
		let (def, ops) = CatalogStore::create_policy(&mut txn, to_create).unwrap();
		assert_eq!(def.name, Some("read_only".to_string()));
		assert_eq!(def.target_type, PolicyTargetType::Table);
		assert!(def.enabled);
		assert_eq!(ops.len(), 1);
		assert_eq!(ops[0].operation, "SELECT");
	}

	#[test]
	fn test_create_policy_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("read_only".to_string()),
				target_type: PolicyTargetType::Table,
				target_namespace: None,
				target_object: None,
				operations: vec![],
			},
		)
		.unwrap();
		let err = CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
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
