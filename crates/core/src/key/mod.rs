// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use cdc_consumer::{CdcConsumerKey, ToConsumerKey};
pub use column::ColumnKey;
pub use column_policy::ColumnPolicyKey;
pub use column_sequence::ColumnSequenceKey;
pub use columns::ColumnsKey;
pub use flow_node_state::{FlowNodeStateKey, FlowNodeStateKeyRange};
pub use index::{IndexKey, SourceIndexKeyRange};
pub use index_entry::IndexEntryKey;
pub use kind::KeyKind;
pub use namespace::NamespaceKey;
pub use namespace_ring_buffer::NamespaceRingBufferKey;
pub use namespace_table::NamespaceTableKey;
pub use namespace_view::NamespaceViewKey;
pub use primary_key::PrimaryKeyKey;
pub use ring_buffer::{RingBufferKey, RingBufferMetadataKey};
pub use row::{RowKey, RowKeyRange};
pub use row_sequence::RowSequenceKey;
pub use system_sequence::SystemSequenceKey;
pub use system_version::{SystemVersion, SystemVersionKey};
pub use table::TableKey;
pub use transaction_version::TransactionVersionKey;
pub use view::ViewKey;

use crate::{EncodedKey, EncodedKeyRange, util::encoding::keycode};

mod cdc_consumer;
mod column;
mod column_policy;
mod column_sequence;
mod columns;
mod flow_node_state;
mod index;
mod index_entry;
mod kind;
mod namespace;
mod namespace_ring_buffer;
mod namespace_table;
mod namespace_view;
mod primary_key;
mod ring_buffer;
mod row;
mod row_sequence;
mod system_sequence;
mod system_version;
mod table;
mod transaction_version;
mod view;

#[derive(Debug)]
pub enum Key {
	CdcConsumer(CdcConsumerKey),
	Namespace(NamespaceKey),
	NamespaceTable(NamespaceTableKey),
	NamespaceView(NamespaceViewKey),
	SystemSequence(SystemSequenceKey),
	Table(TableKey),
	Column(ColumnKey),
	Columns(ColumnsKey),
	Index(IndexKey),
	IndexEntry(IndexEntryKey),
	FlowNodeState(FlowNodeStateKey),
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
}

impl Key {
	pub fn encode(&self) -> EncodedKey {
		match &self {
			Key::CdcConsumer(key) => key.encode(),
			Key::Namespace(key) => key.encode(),
			Key::NamespaceTable(key) => key.encode(),
			Key::NamespaceView(key) => key.encode(),
			Key::Table(key) => key.encode(),
			Key::Column(key) => key.encode(),
			Key::Columns(key) => key.encode(),
			Key::TableColumnPolicy(key) => key.encode(),
			Key::Index(key) => key.encode(),
			Key::IndexEntry(key) => key.encode(),
			Key::FlowNodeState(key) => key.encode(),
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
	pub fn kind(key: &EncodedKey) -> Option<KeyKind> {
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
			KeyKind::Table => TableKey::decode(&key).map(Self::Table),
			KeyKind::Column => ColumnKey::decode(&key).map(Self::Column),
			KeyKind::Index => IndexKey::decode(&key).map(Self::Index),
			KeyKind::IndexEntry => IndexEntryKey::decode(&key).map(Self::IndexEntry),
			KeyKind::FlowNodeState => FlowNodeStateKey::decode(&key).map(Self::FlowNodeState),
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
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::RowNumber;

	use super::{
		ColumnKey, ColumnPolicyKey, ColumnSequenceKey, ColumnsKey, FlowNodeStateKey, Key, NamespaceKey,
		NamespaceTableKey, SystemSequenceKey, TableKey,
	};
	use crate::{
		interface::{
			FlowNodeId, SourceId,
			catalog::{ColumnId, ColumnPolicyId, IndexId, NamespaceId, SequenceId, TableId},
		},
		key::{
			index::IndexKey, row::RowKey, row_sequence::RowSequenceKey,
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
			source: SourceId::table(1),
			column: ColumnId(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Column(decoded_inner) => {
				assert_eq!(decoded_inner.source, SourceId::table(1));
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
			source: SourceId::table(42),
			index: IndexId::primary(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Index(decoded_inner) => {
				assert_eq!(decoded_inner.source, SourceId::table(42));
				assert_eq!(decoded_inner.index, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row() {
		let key = Key::Row(RowKey {
			source: SourceId::table(42),
			row: RowNumber(999_999),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Row(decoded_inner) => {
				assert_eq!(decoded_inner.source, SourceId::table(42));
				assert_eq!(decoded_inner.row, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_row_sequence() {
		let key = Key::RowSequence(RowSequenceKey {
			source: SourceId::table(42),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::RowSequence(decoded_inner) => {
				assert_eq!(decoded_inner.source, SourceId::table(42));
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_sequence() {
		let key = Key::TableColumnSequence(ColumnSequenceKey {
			source: SourceId::table(42),
			column: ColumnId(123),
		});

		let encoded = key.encode();
		let decoded = Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnSequence(decoded_inner) => {
				assert_eq!(decoded_inner.source, SourceId::table(42));
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
