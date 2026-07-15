// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSegmentTreeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_segment_tree(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropSegmentTreeNode,
) -> Result<Columns> {
	let Some(segment_tree_id) = plan.segment_tree_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("segmenttree", Value::Utf8(plan.segment_tree_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_segment_tree(&mut Transaction::Admin(txn), segment_tree_id)?;

	services.catalog.drop_segment_tree(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("segmenttree", Value::Utf8(plan.segment_tree_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
