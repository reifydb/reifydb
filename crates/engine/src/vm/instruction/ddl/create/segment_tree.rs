// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::segment_tree::SegmentTreeToCreate;
use reifydb_core::{
	error::diagnostic::catalog::{monoid_type_not_accepted, unknown_monoid},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateSegmentTreeNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{return_error, value::Value};

use super::require_buffer_for_non_persistent;
use crate::{Result, vm::services::Services};

pub(crate) fn create_segment_tree(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSegmentTreeNode,
) -> Result<Columns> {
	require_buffer_for_non_persistent(txn, plan.persistent, plan.segment_tree.clone(), plan.segment_tree.text())?;

	for aggregate in &plan.aggregates {
		let Some(monoid) = services.routines.get_monoid(&aggregate.monoid) else {
			return_error!(unknown_monoid(plan.segment_tree.clone(), &aggregate.monoid));
		};

		let column = plan
			.columns
			.iter()
			.find(|c| c.name.text() == aggregate.column.as_str())
			.expect("aggregate column already validated by logical plan");
		let column_type = column.constraint.get_type();

		if !monoid.accepted_types().accepts(0, &column_type) {
			return_error!(monoid_type_not_accepted(
				plan.segment_tree.clone(),
				&aggregate.monoid,
				&aggregate.column,
				column_type,
				monoid.accepted_types().expected_at(0),
			));
		}
	}

	let result = services.catalog.create_segment_tree(
		txn,
		SegmentTreeToCreate {
			name: plan.segment_tree.clone(),
			namespace: plan.namespace.def().id(),
			columns: plan.columns,
			key: plan.key,
			aggregates: plan.aggregates,
			partition_by: plan.partition_by,
			underlying: false,
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("segmenttree", Value::Utf8(plan.segment_tree.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::{params::Params, value::Value};

	use crate::{
		test_harness::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_segment_tree() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE SEGMENTTREE test_namespace::cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::sum(load) } }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Uint8(16385));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("cpu".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Boolean(true));

		// Creating the same segment tree again should return error
		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE SEGMENTTREE test_namespace::cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::sum(load) } }",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_segment_tree_unknown_monoid() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE SEGMENTTREE test_namespace::cpu { ts: datetime, load: float8 } WITH { key: ts, aggregates: { total: math::avg(load) } }",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_096");
	}

	#[test]
	fn test_create_segment_tree_type_not_accepted() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE SEGMENTTREE test_namespace::cpu { ts: datetime, name: utf8 } WITH { key: ts, aggregates: { total: math::sum(name) } }",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_097");
	}
}
