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
			policy::{ENABLED, ID, NAME, SHAPE, TARGET_NAMESPACE, TARGET_SHAPE, TARGET_TYPE},
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
		if let Some(ref name) = to_create.name
			&& (Self::find_policy_by_name(&mut Transaction::Admin(&mut *txn), name)?).is_some()
		{
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Policy,
				namespace: "system".to_string(),
				name: name.clone(),
				fragment: Fragment::None,
			}
			.into());
		}

		// Reject unknown operation keys up-front: enforcement matches operation names by
		// exact string equality, so a typo silently makes the whole policy dead code.
		for op in &to_create.operations {
			if !to_create.target_type.is_valid_operation(&op.operation) {
				return Err(CatalogError::PolicyInvalidOperation {
					target_type: to_create.target_type.as_str(),
					operation: op.operation.clone(),
					valid: to_create.target_type.valid_operation_names(),
					policy_name: to_create.name.clone(),
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
		SHAPE.set_utf8(&mut row, TARGET_SHAPE, to_create.target_shape.as_deref().unwrap_or(""));
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
			target_shape: to_create.target_shape,
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
			name: Some("insert_gate".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_shape: None,
			operations: vec![PolicyOpToCreate {
				operation: "insert".to_string(),
				body_source: "require { true }".to_string(),
			}],
		};
		let (def, ops) = CatalogStore::create_policy(&mut txn, to_create).unwrap();
		assert_eq!(def.name, Some("insert_gate".to_string()));
		assert_eq!(def.target_type, PolicyTargetType::Table);
		assert!(def.enabled);
		assert_eq!(ops.len(), 1);
		assert_eq!(ops[0].operation, "insert");
	}

	#[test]
	fn test_create_policy_rejects_typo_on_crud_operation() {
		// `select` is a common typo - RQL reads are `FROM`, not `SELECT`.
		let mut txn = create_test_admin_transaction();
		let err = CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("bad_table".to_string()),
				target_type: PolicyTargetType::Table,
				target_namespace: None,
				target_shape: None,
				operations: vec![PolicyOpToCreate {
					operation: "select".to_string(),
					body_source: "filter { true }".to_string(),
				}],
			},
		)
		.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_086");
	}

	#[test]
	fn test_create_policy_rejects_unknown_session_operation() {
		// `subscribe` is the real-world mistake this validator was added to catch -
		// the session enforcer dispatches on `subscription`, not `subscribe`.
		let mut txn = create_test_admin_transaction();
		let err = CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("bad_session".to_string()),
				target_type: PolicyTargetType::Session,
				target_namespace: None,
				target_shape: None,
				operations: vec![PolicyOpToCreate {
					operation: "subscribe".to_string(),
					body_source: "filter { true }".to_string(),
				}],
			},
		)
		.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_086");
	}

	#[test]
	fn test_create_policy_rejects_op_on_operationless_target_type() {
		// Subscription target type currently admits no operations.
		let mut txn = create_test_admin_transaction();
		let err = CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("bad_sub".to_string()),
				target_type: PolicyTargetType::Subscription,
				target_namespace: None,
				target_shape: None,
				operations: vec![PolicyOpToCreate {
					operation: "from".to_string(),
					body_source: "filter { true }".to_string(),
				}],
			},
		)
		.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_086");
	}

	#[test]
	fn test_create_policy_accepts_from_on_view() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("view_from".to_string()),
				target_type: PolicyTargetType::View,
				target_namespace: None,
				target_shape: None,
				operations: vec![PolicyOpToCreate {
					operation: "from".to_string(),
					body_source: "filter { true }".to_string(),
				}],
			},
		)
		.unwrap();
	}

	#[test]
	fn test_create_policy_allows_empty_operations() {
		// A policy with no operations is meaningless but not malformed - leave that
		// judgment to a separate validator. This test pins the current behaviour.
		let mut txn = create_test_admin_transaction();
		let (def, ops) = CatalogStore::create_policy(
			&mut txn,
			PolicyToCreate {
				name: Some("empty".to_string()),
				target_type: PolicyTargetType::Table,
				target_namespace: None,
				target_shape: None,
				operations: vec![],
			},
		)
		.unwrap();
		assert!(def.enabled);
		assert!(ops.is_empty());
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
				target_shape: None,
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
				target_shape: None,
				operations: vec![],
			},
		)
		.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_042");
	}
}
