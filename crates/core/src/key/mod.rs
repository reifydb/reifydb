// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Typed keys for every catalog object and system structure that ReifyDB persists.
//!
//! Each submodule defines a key type for one logical entry (namespaces, tables, columns, rows, indexes, flows,
//! identities, policies, and so on), plus the cross-cutting `kind` module that enumerates the byte tag for each kind in
//! `KeyKind`. Every key round-trips to and from the canonical byte layout via the order-preserving `keycode` codec in
//! `util/encoding/keycode/`.
//!
//! Invariant: `KeyKind`'s `u8` discriminant is the on-disk key prefix. Reassigning, reordering, or recycling a byte
//! corrupts every persisted database. New kinds must be added by appending a new variant; deletions require an explicit
//! migration and remain reserved for forward compatibility.
//!
//! Invariant: every key type round-trips through `keycode` losslessly, and the codec preserves natural ordering.
//! Storage iteration, range scans, and CDC consumers all rely on the byte order produced by `keycode` matching the
//! natural order of the typed key.

use authentication::AuthenticationKey;
use binding::BindingKey;
use cdc_consumer::CdcConsumerKey;
use column::ColumnKey;
use column_sequence::ColumnSequenceKey;
use columns::ColumnsKey;
use dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey, DictionarySequenceKey};
use flow::FlowKey;
use flow_node_internal_state::FlowNodeInternalStateKey;
use flow_node_state::FlowNodeStateKey;
use granted_role::GrantedRoleKey;
use handler::HandlerKey;
use identity::IdentityKey;
use index::IndexKey;
use index_entry::IndexEntryKey;
use kind::KeyKind;
use namespace::NamespaceKey;
use namespace_binding::NamespaceBindingKey;
use namespace_dictionary::NamespaceDictionaryKey;
use namespace_flow::NamespaceFlowKey;
use namespace_handler::NamespaceHandlerKey;
use namespace_procedure::NamespaceProcedureKey;
use namespace_ringbuffer::NamespaceRingBufferKey;
use namespace_series::NamespaceSeriesKey;
use namespace_sink::NamespaceSinkKey;
use namespace_source::NamespaceSourceKey;
use namespace_sumtype::NamespaceSumTypeKey;
use namespace_table::NamespaceTableKey;
use namespace_view::NamespaceViewKey;
use policy::PolicyKey;
use policy_op::PolicyOpKey;
use primary_key::PrimaryKeyKey;
use procedure::ProcedureKey;
use procedure_param::ProcedureParamKey;
use property::ColumnPropertyKey;
use retention_strategy::{OperatorRetentionStrategyKey, ShapeRetentionStrategyKey};
use ringbuffer::{RingBufferKey, RingBufferMetadataKey};
use role::RoleKey;
use row::RowKey;
use row_sequence::RowSequenceKey;
use series::{SeriesKey, SeriesMetadataKey};
use sink::SinkKey;
use source::SourceKey;
use sumtype::SumTypeKey;
use system_sequence::SystemSequenceKey;
use system_version::SystemVersionKey;
use table::TableKey;
use token::TokenKey;
use transaction_version::TransactionVersionKey;
use view::ViewKey;

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode,
};

pub mod authentication;
pub mod binding;
pub mod cdc_consumer;
pub mod cdc_exclude;
pub mod column;
pub mod column_sequence;
pub mod columns;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod flow_node;
pub mod flow_node_internal_state;
pub mod flow_node_state;
pub mod flow_version;
pub mod granted_role;
pub mod handler;
pub mod identity;
pub mod index;
pub mod index_entry;
pub mod kind;
pub mod migration;
pub mod migration_event;
pub mod namespace;
pub mod namespace_binding;
pub mod namespace_dictionary;
pub mod namespace_flow;
pub mod namespace_handler;
pub mod namespace_procedure;
pub mod namespace_ringbuffer;
pub mod namespace_series;
pub mod namespace_sink;
pub mod namespace_source;
pub mod namespace_sumtype;
pub mod namespace_table;
pub mod namespace_view;
pub mod operator_ttl;
pub mod policy;
pub mod policy_op;
pub mod primary_key;
pub mod procedure;
pub mod procedure_param;
pub mod property;
pub mod retention_strategy;
pub mod ringbuffer;
pub mod role;
pub mod row;
pub mod row_sequence;
pub mod row_ttl;
pub mod series;
pub mod series_row;
pub mod shape;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod system_sequence;
pub mod system_version;
pub mod table;
pub mod token;
pub mod transaction_version;
pub mod variant_handler;
pub mod view;
#[derive(Debug)]
pub enum Key {
	CdcConsumer(CdcConsumerKey),
	Namespace(NamespaceKey),
	NamespaceTable(NamespaceTableKey),
	NamespaceView(NamespaceViewKey),
	NamespaceFlow(NamespaceFlowKey),
	SystemSequence(SystemSequenceKey),
	Table(TableKey),
	Flow(FlowKey),
	Column(ColumnKey),
	Columns(ColumnsKey),
	Index(IndexKey),
	IndexEntry(IndexEntryKey),
	FlowNodeState(FlowNodeStateKey),
	FlowNodeInternalState(FlowNodeInternalStateKey),
	PrimaryKey(PrimaryKeyKey),
	Row(RowKey),
	RowSequence(RowSequenceKey),
	TableColumnSequence(ColumnSequenceKey),
	TableColumnProperty(ColumnPropertyKey),
	SystemVersion(SystemVersionKey),
	TransactionVersion(TransactionVersionKey),
	View(ViewKey),
	RingBuffer(RingBufferKey),
	RingBufferMetadata(RingBufferMetadataKey),
	NamespaceRingBuffer(NamespaceRingBufferKey),
	ShapeRetentionStrategy(ShapeRetentionStrategyKey),
	OperatorRetentionStrategy(OperatorRetentionStrategyKey),
	Dictionary(DictionaryKey),
	DictionaryEntry(DictionaryEntryKey),
	DictionaryEntryIndex(DictionaryEntryIndexKey),
	DictionarySequence(DictionarySequenceKey),
	NamespaceDictionary(NamespaceDictionaryKey),
	SumType(SumTypeKey),
	NamespaceSumType(NamespaceSumTypeKey),
	Handler(HandlerKey),
	NamespaceHandler(NamespaceHandlerKey),
	Series(SeriesKey),
	SeriesMetadata(SeriesMetadataKey),
	NamespaceSeries(NamespaceSeriesKey),
	Identity(IdentityKey),
	Authentication(AuthenticationKey),
	Role(RoleKey),
	GrantedRole(GrantedRoleKey),
	Policy(PolicyKey),
	PolicyOp(PolicyOpKey),
	Token(TokenKey),
	Source(SourceKey),
	NamespaceSource(NamespaceSourceKey),
	Sink(SinkKey),
	NamespaceSink(NamespaceSinkKey),
	Procedure(ProcedureKey),
	NamespaceProcedure(NamespaceProcedureKey),
	ProcedureParam(ProcedureParamKey),
	Binding(BindingKey),
	NamespaceBinding(NamespaceBindingKey),
}

impl Key {
	pub fn encode(&self) -> EncodedKey {
		match &self {
			Key::CdcConsumer(key) => key.encode(),
			Key::Namespace(key) => key.encode(),
			Key::NamespaceTable(key) => key.encode(),
			Key::NamespaceView(key) => key.encode(),
			Key::NamespaceFlow(key) => key.encode(),
			Key::Table(key) => key.encode(),
			Key::Flow(key) => key.encode(),
			Key::Column(key) => key.encode(),
			Key::Columns(key) => key.encode(),
			Key::TableColumnProperty(key) => key.encode(),
			Key::Index(key) => key.encode(),
			Key::IndexEntry(key) => key.encode(),
			Key::FlowNodeState(key) => key.encode(),
			Key::FlowNodeInternalState(key) => key.encode(),
			Key::PrimaryKey(key) => key.encode(),
			Key::Row(key) => key.encode(),
			Key::RowSequence(key) => key.encode(),
			Key::TableColumnSequence(key) => key.encode(),
			Key::SystemSequence(key) => key.encode(),
			Key::SystemVersion(key) => key.encode(),
			Key::TransactionVersion(key) => key.encode(),
			Key::View(key) => key.encode(),
			Key::RingBuffer(key) => key.encode(),
			Key::RingBufferMetadata(key) => key.encode(),
			Key::NamespaceRingBuffer(key) => key.encode(),
			Key::ShapeRetentionStrategy(key) => key.encode(),
			Key::OperatorRetentionStrategy(key) => key.encode(),
			Key::Dictionary(key) => key.encode(),
			Key::DictionaryEntry(key) => key.encode(),
			Key::DictionaryEntryIndex(key) => key.encode(),
			Key::DictionarySequence(key) => key.encode(),
			Key::NamespaceDictionary(key) => key.encode(),
			Key::SumType(key) => key.encode(),
			Key::NamespaceSumType(key) => key.encode(),
			Key::Handler(key) => key.encode(),
			Key::NamespaceHandler(key) => key.encode(),
			Key::Series(key) => key.encode(),
			Key::SeriesMetadata(key) => key.encode(),
			Key::NamespaceSeries(key) => key.encode(),
			Key::Identity(key) => key.encode(),
			Key::Authentication(key) => key.encode(),
			Key::Role(key) => key.encode(),
			Key::GrantedRole(key) => key.encode(),
			Key::Policy(key) => key.encode(),
			Key::PolicyOp(key) => key.encode(),
			Key::Token(key) => key.encode(),
			Key::Source(key) => key.encode(),
			Key::NamespaceSource(key) => key.encode(),
			Key::Sink(key) => key.encode(),
			Key::NamespaceSink(key) => key.encode(),
			Key::Procedure(key) => key.encode(),
			Key::NamespaceProcedure(key) => key.encode(),
			Key::ProcedureParam(key) => key.encode(),
			Key::Binding(key) => key.encode(),
			Key::NamespaceBinding(key) => key.encode(),
		}
	}
}

pub trait EncodableKey {
	const KIND: KeyKind;

	fn encode(&self) -> EncodedKey;

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized;
}

pub trait EncodableKeyRange {
	const KIND: KeyKind;

	fn start(&self) -> Option<EncodedKey>;

	fn end(&self) -> Option<EncodedKey>;

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized;
}

impl Key {
	pub fn kind(key: impl AsRef<[u8]>) -> Option<KeyKind> {
		let key = key.as_ref();
		if key.len() < 2 {
			return None;
		}

		keycode::deserialize(&key[1..2]).ok()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		match kind {
			KeyKind::CdcConsumer => CdcConsumerKey::decode(key).map(Self::CdcConsumer),
			KeyKind::Columns => ColumnsKey::decode(key).map(Self::Columns),
			KeyKind::ColumnProperty => ColumnPropertyKey::decode(key).map(Self::TableColumnProperty),
			KeyKind::Namespace => NamespaceKey::decode(key).map(Self::Namespace),
			KeyKind::NamespaceTable => NamespaceTableKey::decode(key).map(Self::NamespaceTable),
			KeyKind::NamespaceView => NamespaceViewKey::decode(key).map(Self::NamespaceView),
			KeyKind::NamespaceFlow => NamespaceFlowKey::decode(key).map(Self::NamespaceFlow),
			KeyKind::Table => TableKey::decode(key).map(Self::Table),
			KeyKind::Flow => FlowKey::decode(key).map(Self::Flow),
			KeyKind::Column => ColumnKey::decode(key).map(Self::Column),
			KeyKind::Index => IndexKey::decode(key).map(Self::Index),
			KeyKind::IndexEntry => IndexEntryKey::decode(key).map(Self::IndexEntry),
			KeyKind::FlowNodeState => FlowNodeStateKey::decode(key).map(Self::FlowNodeState),
			KeyKind::FlowNodeInternalState => {
				FlowNodeInternalStateKey::decode(key).map(Self::FlowNodeInternalState)
			}
			KeyKind::Row => RowKey::decode(key).map(Self::Row),
			KeyKind::RowSequence => RowSequenceKey::decode(key).map(Self::RowSequence),
			KeyKind::ColumnSequence => ColumnSequenceKey::decode(key).map(Self::TableColumnSequence),
			KeyKind::SystemSequence => SystemSequenceKey::decode(key).map(Self::SystemSequence),
			KeyKind::SystemVersion => SystemVersionKey::decode(key).map(Self::SystemVersion),
			KeyKind::TransactionVersion => TransactionVersionKey::decode(key).map(Self::TransactionVersion),
			KeyKind::View => ViewKey::decode(key).map(Self::View),
			KeyKind::PrimaryKey => PrimaryKeyKey::decode(key).map(Self::PrimaryKey),
			KeyKind::RingBuffer => RingBufferKey::decode(key).map(Self::RingBuffer),
			KeyKind::RingBufferMetadata => RingBufferMetadataKey::decode(key).map(Self::RingBufferMetadata),
			KeyKind::NamespaceRingBuffer => {
				NamespaceRingBufferKey::decode(key).map(Self::NamespaceRingBuffer)
			}
			KeyKind::ShapeRetentionStrategy => {
				ShapeRetentionStrategyKey::decode(key).map(Self::ShapeRetentionStrategy)
			}
			KeyKind::OperatorRetentionStrategy => {
				OperatorRetentionStrategyKey::decode(key).map(Self::OperatorRetentionStrategy)
			}
			KeyKind::FlowNode
			| KeyKind::FlowNodeByFlow
			| KeyKind::FlowEdge
			| KeyKind::FlowEdgeByFlow
			| KeyKind::FlowVersion => None,
			KeyKind::Dictionary => DictionaryKey::decode(key).map(Self::Dictionary),
			KeyKind::DictionaryEntry => DictionaryEntryKey::decode(key).map(Self::DictionaryEntry),
			KeyKind::DictionaryEntryIndex => {
				DictionaryEntryIndexKey::decode(key).map(Self::DictionaryEntryIndex)
			}
			KeyKind::DictionarySequence => DictionarySequenceKey::decode(key).map(Self::DictionarySequence),
			KeyKind::NamespaceDictionary => {
				NamespaceDictionaryKey::decode(key).map(Self::NamespaceDictionary)
			}
			KeyKind::SumType => SumTypeKey::decode(key).map(Self::SumType),
			KeyKind::NamespaceSumType => NamespaceSumTypeKey::decode(key).map(Self::NamespaceSumType),
			KeyKind::Handler => HandlerKey::decode(key).map(Self::Handler),
			KeyKind::NamespaceHandler => NamespaceHandlerKey::decode(key).map(Self::NamespaceHandler),
			KeyKind::VariantHandler => None,
			KeyKind::Metric => None,
			KeyKind::Subscription | KeyKind::SubscriptionColumn | KeyKind::SubscriptionRow => None,
			KeyKind::Shape | KeyKind::RowShapeField => None,
			KeyKind::Series => SeriesKey::decode(key).map(Self::Series),
			KeyKind::NamespaceSeries => NamespaceSeriesKey::decode(key).map(Self::NamespaceSeries),
			KeyKind::SeriesMetadata => SeriesMetadataKey::decode(key).map(Self::SeriesMetadata),
			KeyKind::Identity => IdentityKey::decode(key).map(Self::Identity),
			KeyKind::Authentication => AuthenticationKey::decode(key).map(Self::Authentication),
			KeyKind::Role => RoleKey::decode(key).map(Self::Role),
			KeyKind::GrantedRole => GrantedRoleKey::decode(key).map(Self::GrantedRole),
			KeyKind::Policy => PolicyKey::decode(key).map(Self::Policy),
			KeyKind::PolicyOp => PolicyOpKey::decode(key).map(Self::PolicyOp),
			KeyKind::Migration | KeyKind::MigrationEvent => None,
			KeyKind::Token => TokenKey::decode(key).map(Self::Token),
			KeyKind::ConfigStorage => None,
			KeyKind::Source
			| KeyKind::NamespaceSource
			| KeyKind::Sink
			| KeyKind::NamespaceSink
			| KeyKind::SourceCheckpoint => None,
			KeyKind::RowTtl => None,
			KeyKind::OperatorTtl => None,
			KeyKind::Procedure => ProcedureKey::decode(key).map(Self::Procedure),
			KeyKind::NamespaceProcedure => NamespaceProcedureKey::decode(key).map(Self::NamespaceProcedure),
			KeyKind::ProcedureParam => ProcedureParamKey::decode(key).map(Self::ProcedureParam),
			KeyKind::Binding => BindingKey::decode(key).map(Self::Binding),
			KeyKind::NamespaceBinding => None,
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{row_number::RowNumber, sumtype::SumTypeId};

	use crate::{
		interface::catalog::{
			flow::FlowNodeId,
			id::{ColumnId, ColumnPropertyId, IndexId, NamespaceId, SequenceId, TableId},
			shape::ShapeId,
		},
		key::{
			Key, column::ColumnKey, column_sequence::ColumnSequenceKey, columns::ColumnsKey,
			flow_node_state::FlowNodeStateKey, index::IndexKey, namespace::NamespaceKey,
			namespace_sumtype::NamespaceSumTypeKey, namespace_table::NamespaceTableKey,
			property::ColumnPropertyKey, row::RowKey, row_sequence::RowSequenceKey, sumtype::SumTypeKey,
			system_sequence::SystemSequenceKey, table::TableKey,
			transaction_version::TransactionVersionKey,
		},
	};

	#[test]
	fn test_table_columns() {
		let key = Key::Columns(ColumnsKey {
			column: ColumnId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Columns(decoded_inner) => {
				assert_eq!(decoded_inner.column, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column() {
		let key = Key::Column(ColumnKey {
			shape: ShapeId::table(1),
			column: ColumnId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Column(decoded_inner) => {
				assert_eq!(decoded_inner.shape, ShapeId::table(1));
				assert_eq!(decoded_inner.column, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_property() {
		let key = Key::TableColumnProperty(ColumnPropertyKey {
			column: ColumnId(42),
			property: ColumnPropertyId(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnProperty(decoded_inner) => {
				assert_eq!(decoded_inner.column, 42);
				assert_eq!(decoded_inner.property, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_namespace() {
		let key = Key::Namespace(NamespaceKey {
			namespace: NamespaceId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Namespace(decoded_inner) => {
				assert_eq!(decoded_inner.namespace, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_namespace_table() {
		let key = Key::NamespaceTable(NamespaceTableKey {
			namespace: NamespaceId(42),
			table: TableId(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::NamespaceTable(decoded_inner) => {
				assert_eq!(decoded_inner.namespace, 42);
				assert_eq!(decoded_inner.table, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_system_sequence() {
		let key = Key::SystemSequence(SystemSequenceKey {
			sequence: SequenceId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::SystemSequence(decoded_inner) => {
				assert_eq!(decoded_inner.sequence, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table() {
		let key = Key::Table(TableKey {
			table: TableId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Table(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_index() {
		let key = Key::Index(IndexKey {
			shape: ShapeId::table(42),
			index: IndexId::primary(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Index(decoded_inner) => {
				assert_eq!(decoded_inner.shape, ShapeId::table(42));
				assert_eq!(decoded_inner.index, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row() {
		let key = Key::Row(RowKey {
			shape: ShapeId::table(42),
			row: RowNumber(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Row(decoded_inner) => {
				assert_eq!(decoded_inner.shape, ShapeId::table(42));
				assert_eq!(decoded_inner.row, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row_sequence() {
		let key = Key::RowSequence(RowSequenceKey {
			shape: ShapeId::table(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::RowSequence(decoded_inner) => {
				assert_eq!(decoded_inner.shape, ShapeId::table(42));
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_sequence() {
		let key = Key::TableColumnSequence(ColumnSequenceKey {
			shape: ShapeId::table(42),
			column: ColumnId(123),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnSequence(decoded_inner) => {
				assert_eq!(decoded_inner.shape, ShapeId::table(42));
				assert_eq!(decoded_inner.column, 123);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_transaction_version() {
		let key = Key::TransactionVersion(TransactionVersionKey {});
		let encoded = key.encode();
		Key::decode(&encoded).expect("Failed to decode key");
	}

	#[test]
	fn test_operator_state() {
		let key = Key::FlowNodeState(FlowNodeStateKey {
			node: FlowNodeId(0xCAFEBABE),
			key: vec![1, 2, 3],
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::FlowNodeState(decoded_inner) => {
				assert_eq!(decoded_inner.node, 0xCAFEBABE);
				assert_eq!(decoded_inner.key, vec![1, 2, 3]);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_sumtype_key() {
		let key = Key::SumType(SumTypeKey {
			sumtype: SumTypeId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::SumType(decoded_inner) => {
				assert_eq!(decoded_inner.sumtype, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_namespace_sumtype_key() {
		let key = Key::NamespaceSumType(NamespaceSumTypeKey {
			namespace: NamespaceId(42),
			sumtype: SumTypeId(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::NamespaceSumType(decoded_inner) => {
				assert_eq!(decoded_inner.namespace, 42);
				assert_eq!(decoded_inner.sumtype, 999_999);
			}
			_ => unreachable!(),
		}
	}
}
