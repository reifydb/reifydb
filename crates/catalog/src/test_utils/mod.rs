// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	column::ColumnIndex,
	flow::{Flow, FlowEdge, FlowId, FlowNode, FlowNodeId, FlowStatus},
	handler::Handler,
	id::{RingBufferId, TableId},
	namespace::Namespace,
	property::ColumnPropertyKind,
	ringbuffer::RingBuffer,
	sink::Sink,
	source::Source,
	sumtype::{SumType, SumTypeKind, Variant},
	table::Table,
	view::View,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	fragment::Fragment,
	value::{
		blob::Blob,
		constraint::TypeConstraint,
		sumtype::{SumTypeId, VariantRef},
	},
};

use crate::{
	CatalogStore,
	store::{
		column::create::ColumnToCreate,
		flow::create::FlowToCreate,
		handler::create::HandlerToCreate,
		namespace::create::NamespaceToCreate,
		ringbuffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
		sink::create::SinkToCreate,
		source::create::SourceToCreate,
		table::create::{TableColumnToCreate, TableToCreate},
		view::create::{ViewColumnToCreate, ViewStorageConfig, ViewToCreate},
	},
};

pub fn create_namespace(txn: &mut AdminTransaction, namespace: &str) -> Namespace {
	let local_name = namespace.rsplit_once("::").map(|(_, s)| s).unwrap_or(namespace);
	CatalogStore::create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: None,
			name: namespace.to_string(),
			local_name: local_name.to_string(),
			parent_id: reifydb_core::interface::catalog::id::NamespaceId::ROOT,
			grpc: None,
			token: None,
		},
	)
	.unwrap()
}

pub fn ensure_test_namespace(txn: &mut AdminTransaction) -> Namespace {
	if let Some(result) =
		CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), "test_namespace").unwrap()
	{
		return result;
	}
	create_namespace(txn, "test_namespace")
}

pub fn ensure_test_table(txn: &mut AdminTransaction) -> Table {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) =
		CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut *txn), namespace.id(), "test_table")
			.unwrap()
	{
		return result;
	}
	create_table(txn, "test_namespace", "test_table", &[])
}

pub fn create_table(
	txn: &mut AdminTransaction,
	namespace: &str,
	table: &str,
	columns: &[TableColumnToCreate],
) -> Table {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_table(
		txn,
		TableToCreate {
			name: Fragment::internal(table),
			namespace: namespace.id(),
			columns: columns.to_vec(),
			retention_strategy: None,
			underlying: false,
		},
	)
	.unwrap()
}

pub fn create_test_column(
	txn: &mut AdminTransaction,
	name: &str,
	constraint: TypeConstraint,
	properties: Vec<ColumnPropertyKind>,
) {
	ensure_test_table(txn);

	let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut *txn), TableId(1)).unwrap();

	CatalogStore::create_column(
		txn,
		TableId(1),
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			shape_name: "test_table".to_string(),
			column: name.to_string(),
			constraint,
			properties,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.unwrap();
}

pub fn create_view(txn: &mut AdminTransaction, namespace: &str, view: &str, columns: &[ViewColumnToCreate]) -> View {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_deferred_view(
		txn,
		ViewToCreate {
			name: Fragment::internal(view),
			namespace: namespace.id(),
			columns: columns.to_vec(),
			storage: ViewStorageConfig::default(),
		},
	)
	.unwrap()
}

pub fn ensure_test_ringbuffer(txn: &mut AdminTransaction) -> RingBuffer {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_ringbuffer_by_name(
		&mut Transaction::Admin(&mut *txn),
		namespace.id(),
		"test_ringbuffer",
	)
	.unwrap()
	{
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
) -> RingBuffer {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_ringbuffer(
		txn,
		RingBufferToCreate {
			name: Fragment::internal(ringbuffer),
			namespace: namespace.id(),
			capacity,
			columns: columns.to_vec(),
			partition_by: vec![],
			underlying: false,
		},
	)
	.unwrap()
}

pub fn create_test_ringbuffer_column(
	txn: &mut AdminTransaction,
	ringbuffer_id: RingBufferId,
	name: &str,
	constraint: TypeConstraint,
	properties: Vec<ColumnPropertyKind>,
) {
	let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut *txn), ringbuffer_id).unwrap();

	CatalogStore::create_column(
		txn,
		ringbuffer_id,
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace".to_string(),
			shape_name: "test_ringbuffer".to_string(),
			column: name.to_string(),
			constraint,
			properties,
			index: ColumnIndex(columns.len() as u8),
			auto_increment: false,
			dictionary_id: None,
		},
	)
	.unwrap();
}

pub fn create_flow(txn: &mut AdminTransaction, namespace: &str, flow: &str) -> Flow {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_flow(
		txn,
		FlowToCreate {
			name: Fragment::internal(flow),
			namespace: namespace.id(),
			status: FlowStatus::Active,
			tick: None,
		},
	)
	.unwrap()
}

pub fn ensure_test_flow(txn: &mut AdminTransaction) -> Flow {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) =
		CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut *txn), namespace.id(), "test_flow")
			.unwrap()
	{
		return result;
	}
	create_flow(txn, "test_namespace", "test_flow")
}

pub fn create_flow_node(txn: &mut AdminTransaction, flow_id: FlowId, node_type: u8, data: &[u8]) -> FlowNode {
	use crate::store::sequence::flow::next_flow_node_id;

	let node_id = next_flow_node_id(txn).unwrap();
	let node_def = FlowNode {
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
) -> FlowEdge {
	use crate::store::sequence::flow::next_flow_edge_id;

	let edge_id = next_flow_edge_id(txn).unwrap();
	let edge_def = FlowEdge {
		id: edge_id,
		flow: flow_id,
		source,
		target,
	};

	CatalogStore::create_flow_edge(txn, &edge_def).unwrap();
	edge_def
}

pub fn create_sumtype(txn: &mut AdminTransaction, namespace: &str, name: &str, variants: Vec<Variant>) -> SumType {
	use crate::store::sumtype::create::SumTypeToCreate;

	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_sumtype(
		txn,
		SumTypeToCreate {
			name: Fragment::internal(name),
			namespace: namespace.id(),
			def: SumType {
				id: SumTypeId(0),
				namespace: namespace.id(),
				name: name.to_string(),
				variants,
				kind: SumTypeKind::Enum,
			},
		},
	)
	.unwrap()
}

pub fn create_event(txn: &mut AdminTransaction, namespace: &str, name: &str, variants: Vec<Variant>) -> SumType {
	use crate::store::sumtype::create::SumTypeToCreate;

	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_sumtype(
		txn,
		SumTypeToCreate {
			name: Fragment::internal(name),
			namespace: namespace.id(),
			def: SumType {
				id: SumTypeId(0),
				namespace: namespace.id(),
				name: name.to_string(),
				variants,
				kind: SumTypeKind::Event,
			},
		},
	)
	.unwrap()
}

pub fn create_handler(
	txn: &mut AdminTransaction,
	namespace: &str,
	name: &str,
	variant: VariantRef,
	body_source: &str,
) -> Handler {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");

	CatalogStore::create_handler(
		txn,
		HandlerToCreate {
			name: Fragment::internal(name),
			namespace: namespace.id(),
			variant,
			body_source: body_source.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_sumtype(txn: &mut AdminTransaction) -> SumType {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) =
		CatalogStore::find_sumtype_by_name(&mut Transaction::Admin(&mut *txn), namespace.id(), "test_sumtype")
			.unwrap()
	{
		return result;
	}
	create_sumtype(txn, "test_namespace", "test_sumtype", vec![])
}

pub fn create_source(txn: &mut AdminTransaction, namespace: &str, name: &str, connector: &str) -> Source {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");
	CatalogStore::create_source(
		txn,
		SourceToCreate {
			name: Fragment::internal(name),
			namespace: namespace.id(),
			connector: connector.to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: namespace.id(),
			target_name: "target_table".to_string(),
		},
	)
	.unwrap()
}

pub fn create_sink(txn: &mut AdminTransaction, namespace: &str, name: &str, connector: &str) -> Sink {
	let namespace = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), namespace)
		.unwrap()
		.expect("Namespace not found");
	CatalogStore::create_sink(
		txn,
		SinkToCreate {
			name: Fragment::internal(name),
			namespace: namespace.id(),
			source_namespace: namespace.id(),
			source_name: "source_table".to_string(),
			connector: connector.to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		},
	)
	.unwrap()
}
