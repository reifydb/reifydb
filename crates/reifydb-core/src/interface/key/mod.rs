// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use cdc_consumer::CdcConsumerKey;
pub use cdc_event::CdcEventKey;
pub use column::ColumnKey;
pub use column_policy::ColumnPolicyKey;
pub use kind::KeyKind;
pub use schema::SchemaKey;
pub use schema_table::SchemaTableKey;
pub use system_sequence::SystemSequenceKey;
pub use system_version::{SystemVersion, SystemVersionKey};
pub use table::TableKey;
pub use table_column::TableColumnKey;
pub use table_column_sequence::TableColumnSequenceKey;
pub use table_index::{TableIndexKey, TableIndexKeyRange};
pub use table_index_entry::TableIndexEntryKey;
pub use table_row::{TableRowKey, TableRowKeyRange};
pub use table_row_sequence::TableRowSequenceKey;
pub use transaction_version::TransactionVersionKey;

use crate::{EncodedKey, EncodedKeyRange, util::encoding::keycode};

mod cdc_consumer;
mod cdc_event;
mod column;
mod column_policy;
mod kind;
mod schema;
mod schema_table;
mod system_sequence;
mod system_version;
mod table;
mod table_column;
mod table_column_sequence;
mod table_index;
mod table_index_entry;
mod table_row;
mod table_row_sequence;
mod transaction_version;

#[derive(Debug)]
pub enum Key {
	CdcConsumer(CdcConsumerKey),
	CdcEvent(CdcEventKey),
	Column(ColumnKey),
	ColumnPolicy(ColumnPolicyKey),
	Schema(SchemaKey),
	SchemaTable(SchemaTableKey),
	SystemSequence(SystemSequenceKey),
	Table(TableKey),
	TableColumn(TableColumnKey),
	TableIndex(TableIndexKey),
	TableIndexEntry(TableIndexEntryKey),
	TableRow(TableRowKey),
	TableRowSequence(TableRowSequenceKey),
	TableColumnSequence(TableColumnSequenceKey),
	SystemVersion(SystemVersionKey),
	TransactionVersion(TransactionVersionKey),
}

impl Key {
	pub fn encode(&self) -> EncodedKey {
		match &self {
			Key::CdcConsumer(key) => key.encode(),
			Key::CdcEvent(key) => key.encode(),
			Key::Column(key) => key.encode(),
			Key::ColumnPolicy(key) => key.encode(),
			Key::Schema(key) => key.encode(),
			Key::SchemaTable(key) => key.encode(),
			Key::Table(key) => key.encode(),
			Key::TableColumn(key) => key.encode(),
			Key::TableIndex(key) => key.encode(),
			Key::TableIndexEntry(key) => key.encode(),
			Key::TableRow(key) => key.encode(),
			Key::TableRowSequence(key) => key.encode(),
			Key::TableColumnSequence(key) => key.encode(),
			Key::SystemSequence(key) => key.encode(),
			Key::SystemVersion(key) => key.encode(),
			Key::TransactionVersion(key) => key.encode(),
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
	pub fn decode(key: &EncodedKey) -> Option<Self> {
		if key.len() < 2 {
			return None;
		}

		let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
		match kind {
			KeyKind::CdcConsumer => CdcConsumerKey::decode(&key)
				.map(Self::CdcConsumer),
			KeyKind::CdcEvent => {
				CdcEventKey::decode(&key).map(Self::CdcEvent)
			}
			KeyKind::Column => {
				ColumnKey::decode(&key).map(Self::Column)
			}
			KeyKind::ColumnPolicy => ColumnPolicyKey::decode(&key)
				.map(Self::ColumnPolicy),
			KeyKind::Schema => {
				SchemaKey::decode(&key).map(Self::Schema)
			}
			KeyKind::SchemaTable => SchemaTableKey::decode(&key)
				.map(Self::SchemaTable),
			KeyKind::Table => {
				TableKey::decode(&key).map(Self::Table)
			}
			KeyKind::TableColumn => TableColumnKey::decode(&key)
				.map(Self::TableColumn),
			KeyKind::TableIndex => TableIndexKey::decode(&key)
				.map(Self::TableIndex),
			KeyKind::TableIndexEntry => {
				TableIndexEntryKey::decode(&key)
					.map(Self::TableIndexEntry)
			}
			KeyKind::TableRow => {
				TableRowKey::decode(&key).map(Self::TableRow)
			}
			KeyKind::TableRowSequence => {
				TableRowSequenceKey::decode(&key)
					.map(Self::TableRowSequence)
			}
			KeyKind::TableColumnSequence => {
				TableColumnSequenceKey::decode(&key)
					.map(Self::TableColumnSequence)
			}
			KeyKind::SystemSequence => {
				SystemSequenceKey::decode(&key)
					.map(Self::SystemSequence)
			}
			KeyKind::SystemVersion => {
				SystemVersionKey::decode(&key)
					.map(Self::SystemVersion)
			}
			KeyKind::TransactionVersion => {
				TransactionVersionKey::decode(&key)
					.map(Self::TransactionVersion)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{
		CdcEventKey, ColumnKey, ColumnPolicyKey, Key, SchemaKey,
		SchemaTableKey, SystemSequenceKey, TableColumnKey,
		TableColumnSequenceKey, TableIndexKey, TableKey, TableRowKey,
		TableRowSequenceKey,
	};
	use crate::{
		RowId,
		interface::{
			catalog::{
				ColumnId, ColumnPolicyId, IndexId, SchemaId,
				SystemSequenceId, TableId,
			},
			key::transaction_version::TransactionVersionKey,
		},
	};

	#[test]
	fn test_column() {
		let key = Key::Column(ColumnKey {
			column: ColumnId(42),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Column(decoded_inner) => {
				assert_eq!(decoded_inner.column, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_column_policy() {
		let key = Key::ColumnPolicy(ColumnPolicyKey {
			column: ColumnId(42),
			policy: ColumnPolicyId(999_999),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::ColumnPolicy(decoded_inner) => {
				assert_eq!(decoded_inner.column, 42);
				assert_eq!(decoded_inner.policy, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_schema() {
		let key = Key::Schema(SchemaKey {
			schema: SchemaId(42),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Schema(decoded_inner) => {
				assert_eq!(decoded_inner.schema, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_schema_table() {
		let key = Key::SchemaTable(SchemaTableKey {
			schema: SchemaId(42),
			table: TableId(999_999),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::SchemaTable(decoded_inner) => {
				assert_eq!(decoded_inner.schema, 42);
				assert_eq!(decoded_inner.table, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_system_sequence() {
		let key = Key::SystemSequence(SystemSequenceKey {
			sequence: SystemSequenceId(42),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

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
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::Table(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table_column() {
		let key = Key::TableColumn(TableColumnKey {
			table: TableId(42),
			column: ColumnId(999_999),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumn(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
				assert_eq!(decoded_inner.column, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table_index() {
		let key = Key::TableIndex(TableIndexKey {
			table: TableId(42),
			index: IndexId(999_999),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableIndex(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
				assert_eq!(decoded_inner.index, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table_row() {
		let key = Key::TableRow(TableRowKey {
			table: TableId(42),
			row: RowId(999_999),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableRow(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
				assert_eq!(decoded_inner.row, 999_999);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table_row_sequence() {
		let key = Key::TableRowSequence(TableRowSequenceKey {
			table: TableId(42),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableRowSequence(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
			}
			_ => unreachable!(),
		}
	}

	#[test]
	fn test_table_column_sequence() {
		let key = Key::TableColumnSequence(TableColumnSequenceKey {
			table: TableId(42),
			column: ColumnId(123),
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::TableColumnSequence(decoded_inner) => {
				assert_eq!(decoded_inner.table, 42);
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
	fn test_cdc_event() {
		let key = Key::CdcEvent(CdcEventKey {
			version: 123456789,
			sequence: 999,
		});

		let encoded = key.encode();
		let decoded =
			Key::decode(&encoded).expect("Failed to decode key");

		match decoded {
			Key::CdcEvent(decoded_inner) => {
				assert_eq!(decoded_inner.version, 123456789);
				assert_eq!(decoded_inner.sequence, 999);
			}
			_ => unreachable!(),
		}
	}
}
