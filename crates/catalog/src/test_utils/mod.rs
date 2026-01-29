// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	column::ColumnIndex,
	flow::{FlowDef, FlowEdgeDef, FlowId, FlowNodeDef, FlowNodeId, FlowStatus},
	id::{RingBufferId, TableId},
	namespace::NamespaceDef,
	policy::ColumnPolicyKind,
	ringbuffer::RingBufferDef,
	table::TableDef,
	view::ViewDef,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{blob::Blob, constraint::TypeConstraint};

use crate::{
	CatalogStore,
	store::{
		column::create::ColumnToCreate,
		flow::create::FlowToCreate,
		namespace::create::NamespaceToCreate,
		ringbuffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
		table::create::{TableColumnToCreate, TableToCreate},
		view::create::{ViewColumnToCreate, ViewToCreate},
	},
};

pub fn create_namespace(txn: &mut AdminTransaction, namespace: &str) -> NamespaceDef {
	CatalogStore::create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: None,
			name: namespace.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_namespace(txn: &mut AdminTransaction) -> NamespaceDef {
	if let Some(result) = CatalogStore::find_namespace_by_name(txn, "test_namespace").unwrap() {
		return result;
	}
	create_namespace(txn, "test_namespace")
}

pub fn ensure_test_table(txn: &mut AdminTransaction) -> TableDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_table_by_name(txn, namespace.id, "test_table").unwrap() {
		return result;
	}
	create_table(txn, "test_namespace", "test_table", &[])
}

pub fn create_table(
	txn: &mut AdminTransaction,
	namespace: &str,
	table: &str,
	columns: &[TableColumnToCreate],
) -> TableDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_table(
		txn,
		TableToCreate {
			fragment: None,
			table: table.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
			retention_policy: None,
		},
	)
	.unwrap()
}

pub fn create_test_column(
	txn: &mut AdminTransaction,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn);

	let columns = CatalogStore::list_columns(txn, TableId(1)).unwrap();

	CatalogStore::create_column(
		txn,
		TableId(1),
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			primitive_name: "test_table".to_string(),
			column: name.to_string(),
			constraint,
			policies,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.unwrap();
}

pub fn create_view(txn: &mut AdminTransaction, namespace: &str, view: &str, columns: &[ViewColumnToCreate]) -> ViewDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_deferred_view(
		txn,
		ViewToCreate {
			fragment: None,
			name: view.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn ensure_test_ringbuffer(txn: &mut AdminTransaction) -> RingBufferDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_ringbuffer_by_name(txn, namespace.id, "test_ringbuffer").unwrap() {
		return result;
	}
	create_ringbuffer(txn, "test_namespace", "test_ringbuffer", 100, &[])
}

pub fn create_ringbuffer(
	txn: &mut AdminTransaction,
	namespace: &str,
	ringbuffer: &str,
	capacity: u64,
	columns: &[RingBufferColumnToCreate],
) -> RingBufferDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_ringbuffer(
		txn,
		RingBufferToCreate {
			fragment: None,
			ringbuffer: ringbuffer.to_string(),
			namespace: namespace_def.id,
			capacity,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn create_test_ringbuffer_column(
	txn: &mut AdminTransaction,
	ringbuffer_id: RingBufferId,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	let columns = CatalogStore::list_columns(txn, ringbuffer_id).unwrap();

	CatalogStore::create_column(
		txn,
		ringbuffer_id,
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			primitive_name: "test_ringbuffer".to_string(),
			column: name.to_string(),
			constraint,
			policies,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.unwrap();
}

pub fn create_flow(txn: &mut AdminTransaction, namespace: &str, flow: &str) -> FlowDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_flow(
		txn,
		FlowToCreate {
			fragment: None,
			name: flow.to_string(),
			namespace: namespace_def.id,
			status: FlowStatus::Active,
		},
	)
	.unwrap()
}

pub fn ensure_test_flow(txn: &mut AdminTransaction) -> FlowDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_flow_by_name(txn, namespace.id, "test_flow").unwrap() {
		return result;
	}
	create_flow(txn, "test_namespace", "test_flow")
}

pub fn create_flow_node(txn: &mut AdminTransaction, flow_id: FlowId, node_type: u8, data: &[u8]) -> FlowNodeDef {
	use crate::store::sequence::flow::next_flow_node_id;

	let node_id = next_flow_node_id(txn).unwrap();
	let node_def = FlowNodeDef {
		id: node_id,
		flow: flow_id,
		node_type,
		data: Blob::from(data),
	};

	CatalogStore::create_flow_node(txn, &node_def).unwrap();
	node_def
}

pub fn create_flow_edge(
	txn: &mut AdminTransaction,
	flow_id: FlowId,
	source: FlowNodeId,
	target: FlowNodeId,
) -> FlowEdgeDef {
	use crate::store::sequence::flow::next_flow_edge_id;

	let edge_id = next_flow_edge_id(txn).unwrap();
	let edge_def = FlowEdgeDef {
		id: edge_id,
		flow: flow_id,
		source,
		target,
	};

	CatalogStore::create_flow_edge(txn, &edge_def).unwrap();
	edge_def
}
