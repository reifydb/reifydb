// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use cdc_consumer::CdcConsumerKey;
use column::ColumnKey;
use column_policy::ColumnPolicyKey;
use column_sequence::ColumnSequenceKey;
use columns::ColumnsKey;
use dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey, DictionarySequenceKey};
use flow::FlowKey;
use flow_node_internal_state::FlowNodeInternalStateKey;
use flow_node_state::FlowNodeStateKey;
use index::IndexKey;
use index_entry::IndexEntryKey;
use kind::KeyKind;
use namespace::NamespaceKey;
use namespace_dictionary::NamespaceDictionaryKey;
use namespace_flow::NamespaceFlowKey;
use namespace_ringbuffer::NamespaceRingBufferKey;
use namespace_table::NamespaceTableKey;
use namespace_view::NamespaceViewKey;
use primary_key::PrimaryKeyKey;
use retention_policy::{OperatorRetentionPolicyKey, PrimitiveRetentionPolicyKey};
use ringbuffer::{RingBufferKey, RingBufferMetadataKey};
use row::RowKey;
use row_sequence::RowSequenceKey;
pub use schema::{SchemaFieldKey, SchemaKey};
use subscription::SubscriptionKey;
use subscription_column::SubscriptionColumnKey;
use subscription_row::SubscriptionRowKey;
use system_sequence::SystemSequenceKey;
use system_version::SystemVersionKey;
use table::TableKey;
use transaction_version::TransactionVersionKey;
use view::ViewKey;

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode,
};

pub mod cdc_consumer;
pub mod cdc_exclude;
pub mod column;
pub mod column_policy;
pub mod column_sequence;
pub mod columns;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod flow_node;
pub mod flow_node_internal_state;
pub mod flow_node_state;
pub mod flow_version;
pub mod index;
pub mod index_entry;
pub mod kind;
pub mod namespace;
pub mod namespace_dictionary;
pub mod namespace_flow;
pub mod namespace_ringbuffer;
pub mod namespace_table;
pub mod namespace_view;
pub mod primary_key;
pub mod retention_policy;
pub mod ringbuffer;
pub mod row;
pub mod row_sequence;
pub mod schema;
pub mod subscription;
pub mod subscription_column;
pub mod subscription_row;
pub mod system_sequence;
pub mod system_version;
pub mod table;
pub mod transaction_version;
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
	TableColumnPolicy(ColumnPolicyKey),
	SystemVersion(SystemVersionKey),
	TransactionVersion(TransactionVersionKey),
	View(ViewKey),
	RingBuffer(RingBufferKey),
	RingBufferMetadata(RingBufferMetadataKey),
	NamespaceRingBuffer(NamespaceRingBufferKey),
	PrimitiveRetentionPolicy(PrimitiveRetentionPolicyKey),
	OperatorRetentionPolicy(OperatorRetentionPolicyKey),
	Dictionary(DictionaryKey),
	DictionaryEntry(DictionaryEntryKey),
	DictionaryEntryIndex(DictionaryEntryIndexKey),
	DictionarySequence(DictionarySequenceKey),
	NamespaceDictionary(NamespaceDictionaryKey),
	Subscription(SubscriptionKey),
	SubscriptionColumn(SubscriptionColumnKey),
	SubscriptionRow(SubscriptionRowKey),
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
			Key::TableColumnPolicy(key) => key.encode(),
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
			Key::PrimitiveRetentionPolicy(key) => key.encode(),
			Key::OperatorRetentionPolicy(key) => key.encode(),
			Key::Dictionary(key) => key.encode(),
			Key::DictionaryEntry(key) => key.encode(),
			Key::DictionaryEntryIndex(key) => key.encode(),
			Key::DictionarySequence(key) => key.encode(),
			Key::NamespaceDictionary(key) => key.encode(),
			Key::Subscription(key) => key.encode(),
			Key::SubscriptionColumn(key) => key.encode(),
			Key::SubscriptionRow(key) => key.encode(),
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
			KeyKind::CdcConsumer => CdcConsumerKey::decode(&key).map(Self::CdcConsumer),
			KeyKind::Columns => ColumnsKey::decode(&key).map(Self::Columns),
			KeyKind::ColumnPolicy => ColumnPolicyKey::decode(&key).map(Self::TableColumnPolicy),
			KeyKind::Namespace => NamespaceKey::decode(&key).map(Self::Namespace),
			KeyKind::NamespaceTable => NamespaceTableKey::decode(&key).map(Self::NamespaceTable),
			KeyKind::NamespaceView => NamespaceViewKey::decode(&key).map(Self::NamespaceView),
			KeyKind::NamespaceFlow => NamespaceFlowKey::decode(&key).map(Self::NamespaceFlow),
			KeyKind::Table => TableKey::decode(&key).map(Self::Table),
			KeyKind::Flow => FlowKey::decode(&key).map(Self::Flow),
			KeyKind::Column => ColumnKey::decode(&key).map(Self::Column),
			KeyKind::Index => IndexKey::decode(&key).map(Self::Index),
			KeyKind::IndexEntry => IndexEntryKey::decode(&key).map(Self::IndexEntry),
			KeyKind::FlowNodeState => FlowNodeStateKey::decode(&key).map(Self::FlowNodeState),
			KeyKind::FlowNodeInternalState => {
				FlowNodeInternalStateKey::decode(&key).map(Self::FlowNodeInternalState)
			}
			KeyKind::Row => RowKey::decode(&key).map(Self::Row),
			KeyKind::RowSequence => RowSequenceKey::decode(&key).map(Self::RowSequence),
			KeyKind::ColumnSequence => ColumnSequenceKey::decode(&key).map(Self::TableColumnSequence),
			KeyKind::SystemSequence => SystemSequenceKey::decode(&key).map(Self::SystemSequence),
			KeyKind::SystemVersion => SystemVersionKey::decode(&key).map(Self::SystemVersion),
			KeyKind::TransactionVersion => {
				TransactionVersionKey::decode(&key).map(Self::TransactionVersion)
			}
			KeyKind::View => ViewKey::decode(&key).map(Self::View),
			KeyKind::PrimaryKey => PrimaryKeyKey::decode(&key).map(Self::PrimaryKey),
			KeyKind::RingBuffer => RingBufferKey::decode(&key).map(Self::RingBuffer),
			KeyKind::RingBufferMetadata => {
				RingBufferMetadataKey::decode(&key).map(Self::RingBufferMetadata)
			}
			KeyKind::NamespaceRingBuffer => {
				NamespaceRingBufferKey::decode(&key).map(Self::NamespaceRingBuffer)
			}
			KeyKind::PrimitiveRetentionPolicy => {
				PrimitiveRetentionPolicyKey::decode(&key).map(Self::PrimitiveRetentionPolicy)
			}
			KeyKind::OperatorRetentionPolicy => {
				OperatorRetentionPolicyKey::decode(&key).map(Self::OperatorRetentionPolicy)
			}
			KeyKind::FlowNode
			| KeyKind::FlowNodeByFlow
			| KeyKind::FlowEdge
			| KeyKind::FlowEdgeByFlow
			| KeyKind::FlowVersion => {
				// These keys are used directly via EncodableKey trait, not through Key enum
				None
			}
			KeyKind::Dictionary => DictionaryKey::decode(&key).map(Self::Dictionary),
			KeyKind::DictionaryEntry => DictionaryEntryKey::decode(&key).map(Self::DictionaryEntry),
			KeyKind::DictionaryEntryIndex => {
				DictionaryEntryIndexKey::decode(&key).map(Self::DictionaryEntryIndex)
			}
			KeyKind::DictionarySequence => {
				DictionarySequenceKey::decode(&key).map(Self::DictionarySequence)
			}
			KeyKind::NamespaceDictionary => {
				NamespaceDictionaryKey::decode(&key).map(Self::NamespaceDictionary)
			}
			KeyKind::Metric => {
				// Storage tracker keys are used for internal persistence, not through Key enum
				None
			}
			KeyKind::Subscription => SubscriptionKey::decode(&key).map(Self::Subscription),
			KeyKind::SubscriptionColumn => {
				SubscriptionColumnKey::decode(&key).map(Self::SubscriptionColumn)
			}
			KeyKind::SubscriptionRow => SubscriptionRowKey::decode(&key).map(Self::SubscriptionRow),
			KeyKind::Schema | KeyKind::SchemaField => {
				// Schema keys are used directly via EncodableKey trait, not through Key enum
				None
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::row_number::RowNumber;

	use crate::{
		interface::catalog::{
			flow::FlowNodeId,
			id::{ColumnId, ColumnPolicyId, IndexId, NamespaceId, SequenceId, TableId},
			primitive::PrimitiveId,
		},
		key::{
			Key, column::ColumnKey, column_policy::ColumnPolicyKey, column_sequence::ColumnSequenceKey,
			columns::ColumnsKey, flow_node_state::FlowNodeStateKey, index::IndexKey,
			namespace::NamespaceKey, namespace_table::NamespaceTableKey, row::RowKey,
			row_sequence::RowSequenceKey, system_sequence::SystemSequenceKey, table::TableKey,
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
			primitive: PrimitiveId::table(1),
			column: ColumnId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Column(decoded_inner) => {
				assert_eq!(decoded_inner.primitive, PrimitiveId::table(1));
				assert_eq!(decoded_inner.column, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_policy() {
		let key = Key::TableColumnPolicy(ColumnPolicyKey {
			column: ColumnId(42),
			policy: ColumnPolicyId(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnPolicy(decoded_inner) => {
				assert_eq!(decoded_inner.column, 42);
				assert_eq!(decoded_inner.policy, 999_999);
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
			primitive: PrimitiveId::table(42),
			index: IndexId::primary(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Index(decoded_inner) => {
				assert_eq!(decoded_inner.primitive, PrimitiveId::table(42));
				assert_eq!(decoded_inner.index, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row() {
		let key = Key::Row(RowKey {
			primitive: PrimitiveId::table(42),
			row: RowNumber(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Row(decoded_inner) => {
				assert_eq!(decoded_inner.primitive, PrimitiveId::table(42));
				assert_eq!(decoded_inner.row, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row_sequence() {
		let key = Key::RowSequence(RowSequenceKey {
			primitive: PrimitiveId::table(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::RowSequence(decoded_inner) => {
				assert_eq!(decoded_inner.primitive, PrimitiveId::table(42));
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_sequence() {
		let key = Key::TableColumnSequence(ColumnSequenceKey {
			primitive: PrimitiveId::table(42),
			column: ColumnId(123),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnSequence(decoded_inner) => {
				assert_eq!(decoded_inner.primitive, PrimitiveId::table(42));
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
}
