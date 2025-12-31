// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	ColumnPolicyKind, FlowDef, FlowEdgeDef, FlowId, FlowNodeDef, FlowNodeId, FlowStatus, NamespaceDef,
	RingBufferDef, RingBufferId, TableDef, TableId, ViewDef,
};
use reifydb_transaction::StandardCommandTransaction;
use reifydb_type::{Blob, TypeConstraint};

use crate::{
	CatalogStore,
	store::{
		column::{ColumnIndex, ColumnToCreate},
		flow::create::FlowToCreate,
		namespace::NamespaceToCreate,
		ringbuffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
		table::TableToCreate,
		view::ViewToCreate,
	},
};

pub async fn create_namespace(txn: &mut StandardCommandTransaction, namespace: &str) -> NamespaceDef {
	CatalogStore::create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: None,
			name: namespace.to_string(),
		},
	)
	.await
	.unwrap()
}

pub async fn ensure_test_namespace(txn: &mut StandardCommandTransaction) -> NamespaceDef {
	if let Some(result) = CatalogStore::find_namespace_by_name(txn, "test_namespace").await.unwrap() {
		return result;
	}
	create_namespace(txn, "test_namespace").await
}

pub async fn ensure_test_table(txn: &mut StandardCommandTransaction) -> TableDef {
	let namespace = ensure_test_namespace(txn).await;

	if let Some(result) = CatalogStore::find_table_by_name(txn, namespace.id, "test_table").await.unwrap() {
		return result;
	}
	create_table(txn, "test_namespace", "test_table", &[]).await
}

pub async fn create_table(
	txn: &mut StandardCommandTransaction,
	namespace: &str,
	table: &str,
	columns: &[crate::store::table::TableColumnToCreate],
) -> TableDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace).await.unwrap().expect("Namespace not found");

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
	.await
	.unwrap()
}

pub async fn create_test_column(
	txn: &mut StandardCommandTransaction,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn).await;

	let columns = CatalogStore::list_columns(txn, TableId(1)).await.unwrap();

	CatalogStore::create_column(
		txn,
		TableId(1),
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			table: TableId(1025),
			table_name: "test_table".to_string(),
			column: name.to_string(),
			constraint,
			if_not_exists: false,
			policies,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.await
	.unwrap();
}

pub async fn create_view(
	txn: &mut StandardCommandTransaction,
	namespace: &str,
	view: &str,
	columns: &[crate::store::view::ViewColumnToCreate],
) -> ViewDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace).await.unwrap().expect("Namespace not found");

	CatalogStore::create_deferred_view(
		txn,
		ViewToCreate {
			fragment: None,
			name: view.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
		},
	)
	.await
	.unwrap()
}

pub async fn ensure_test_ringbuffer(txn: &mut StandardCommandTransaction) -> RingBufferDef {
	let namespace = ensure_test_namespace(txn).await;

	if let Some(result) = CatalogStore::find_ringbuffer_by_name(txn, namespace.id, "test_ringbuffer").await.unwrap()
	{
		return result;
	}
	create_ringbuffer(txn, "test_namespace", "test_ringbuffer", 100, &[]).await
}

pub async fn create_ringbuffer(
	txn: &mut StandardCommandTransaction,
	namespace: &str,
	ringbuffer: &str,
	capacity: u64,
	columns: &[RingBufferColumnToCreate],
) -> RingBufferDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace).await.unwrap().expect("Namespace not found");

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
	.await
	.unwrap()
}

pub async fn create_test_ringbuffer_column(
	txn: &mut StandardCommandTransaction,
	ringbuffer_id: RingBufferId,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	let columns = CatalogStore::list_columns(txn, ringbuffer_id).await.unwrap();

	CatalogStore::create_column(
		txn,
		ringbuffer_id,
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			table: TableId(0),
			table_name: "test_ringbuffer".to_string(),
			column: name.to_string(),
			constraint,
			if_not_exists: false,
			policies,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.await
	.unwrap();
}

pub async fn create_flow(txn: &mut StandardCommandTransaction, namespace: &str, flow: &str) -> FlowDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace).await.unwrap().expect("Namespace not found");

	CatalogStore::create_flow(
		txn,
		FlowToCreate {
			fragment: None,
			name: flow.to_string(),
			namespace: namespace_def.id,
			status: FlowStatus::Active,
		},
	)
	.await
	.unwrap()
}

pub async fn ensure_test_flow(txn: &mut StandardCommandTransaction) -> FlowDef {
	let namespace = ensure_test_namespace(txn).await;

	if let Some(result) = CatalogStore::find_flow_by_name(txn, namespace.id, "test_flow").await.unwrap() {
		return result;
	}
	create_flow(txn, "test_namespace", "test_flow").await
}

pub async fn create_flow_node(
	txn: &mut StandardCommandTransaction,
	flow_id: FlowId,
	node_type: u8,
	data: &[u8],
) -> FlowNodeDef {
	use crate::store::sequence::flow::next_flow_node_id;

	let node_id = next_flow_node_id(txn).await.unwrap();
	let node_def = FlowNodeDef {
		id: node_id,
		flow: flow_id,
		node_type,
		data: Blob::from(data),
	};

	CatalogStore::create_flow_node(txn, &node_def).await.unwrap();
	node_def
}

pub async fn create_flow_edge(
	txn: &mut StandardCommandTransaction,
	flow_id: FlowId,
	source: FlowNodeId,
	target: FlowNodeId,
) -> FlowEdgeDef {
	use crate::store::sequence::flow::next_flow_edge_id;

	let edge_id = next_flow_edge_id(txn).await.unwrap();
	let edge_def = FlowEdgeDef {
		id: edge_id,
		flow: flow_id,
		source,
		target,
	};

	CatalogStore::create_flow_edge(txn, &edge_def).await.unwrap();
	edge_def
}
