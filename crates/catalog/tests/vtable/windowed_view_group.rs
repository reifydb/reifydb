// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::windowed_view_group::WindowedViewGroupToCreate;
use reifydb_core::{
	durability::Durability,
	interface::catalog::{flow::FlowId, id::ViewId},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{column_view::ColumnView, frame::frame::Frame, identity::IdentityId},
};

#[test]
fn vtables_list_one_group_row_and_one_row_per_member_in_declaration_order() {
	// A group must surface once in the group table and once per member, members in declaration order.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE wvg_vtable_a");
	let catalog = t.catalog();

	let group = {
		let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let namespace = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "wvg_vtable_a")
			.unwrap()
			.unwrap()
			.id();
		let group = catalog
			.create_windowed_view_group(
				&mut txn,
				WindowedViewGroupToCreate {
					name: Fragment::internal("ohlcv"),
					namespace,
					flow: FlowId(42),
					members: vec![ViewId(30), ViewId(10), ViewId(20)],
				},
			)
			.unwrap();
		txn.commit().unwrap();
		group
	};

	let frames = t.query("from system::windowed_view_groups");
	let frame = frames.first().expect("expected at least one frame");
	assert_eq!(column(frame, "id").len(), 1, "expected exactly one group row");
	assert_eq!(column(frame, "id").as_string(0), group.id.0.to_string());
	assert_eq!(column(frame, "namespace_id").as_string(0), group.namespace.0.to_string());
	assert_eq!(column(frame, "name").as_string(0), "ohlcv");
	assert_eq!(column(frame, "flow_id").as_string(0), "42");

	let frames = t.query("from system::windowed_view_group_members");
	let frame = frames.first().expect("expected at least one frame");
	let group_ids = column(frame, "group_id");
	let view_ids = column(frame, "view_id");
	assert_eq!(view_ids.len(), 3, "expected one row per member");
	for i in 0..3 {
		assert_eq!(group_ids.as_string(i), group.id.0.to_string());
	}
	let members: Vec<String> = (0..3).map(|i| view_ids.as_string(i)).collect();
	assert_eq!(members, vec!["30", "10", "20"]);
}

fn column<'a>(frame: &'a Frame, name: &str) -> ColumnView<'a> {
	frame.try_column(name).unwrap_or_else(|e| panic!("missing column {name}: {e:?}"))
}
