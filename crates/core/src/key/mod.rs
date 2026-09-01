// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use kind::KeyKind;
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};

pub mod catalog;
pub mod cdc;
pub mod column;
pub mod config;
pub mod flow;
pub mod identity;
pub mod kind;
pub mod namespace;
pub mod operator;
pub mod operator_settings;
pub mod output_frontier;
pub mod partition;
pub mod procedure;
pub mod queue;
pub mod ringbuffer;
pub mod row;
pub mod series;
pub mod system;
pub mod typed;

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

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::{row_number::RowNumber, sumtype::SumTypeId};

	use crate::{
		interface::catalog::{
			flow::OperatorId,
			id::{ColumnId, ColumnPropertyId, IndexId, NamespaceId, RelationshipId, SequenceId, TableId},
			object::ObjectId,
			storage::StorageId,
		},
		key::{
			EncodableKey,
			catalog::{ColumnPropertyKey, IndexKey, RelationshipKey, SumTypeKey, TableKey},
			column::{ColumnKey, ColumnSequenceKey, ColumnsKey},
			namespace::{NamespaceKey, NamespaceSumTypeKey, NamespaceTableKey},
			operator::state::{GroupId, KeyspaceId, OperatorStateKey},
			row::{RowKey, RowSequenceKey},
			series::SeriesRowKey,
			system::{SystemSequenceKey, TransactionVersionKey},
			typed::key::Key,
		},
	};

	#[test]
	fn test_table_columns() {
		let key = ColumnsKey {
			column: ColumnId(42),
		};

		let encoded = key.encode();
		let decoded = ColumnsKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.column, 42);
	}

	#[test]
	fn test_column() {
		let key = ColumnKey {
			object: ObjectId::table(1),
			column: ColumnId(42),
		};

		let encoded = key.encode();
		let decoded = ColumnKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.object, ObjectId::table(1));
		assert_eq!(decoded.column, 42);
	}

	#[test]
	fn test_column_property() {
		let key = ColumnPropertyKey {
			column: ColumnId(42),
			property: ColumnPropertyId(999_999),
		};

		let encoded = key.encode();
		let decoded = ColumnPropertyKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.column, 42);
		assert_eq!(decoded.property, 999_999);
	}

	#[test]
	fn test_namespace() {
		let key = NamespaceKey {
			namespace: NamespaceId(42),
		};

		let encoded = key.encode();
		let decoded = NamespaceKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.namespace, 42);
	}

	#[test]
	fn test_namespace_table() {
		let key = NamespaceTableKey {
			namespace: NamespaceId(42),
			table: TableId(999_999),
		};

		let encoded = key.encode();
		let decoded = NamespaceTableKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.namespace, 42);
		assert_eq!(decoded.table, 999_999);
	}

	#[test]
	fn test_system_sequence() {
		let key = SystemSequenceKey {
			sequence: SequenceId(42),
		};

		let encoded = key.encode();
		let decoded = SystemSequenceKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.sequence, 42);
	}

	#[test]
	fn test_table() {
		let key = TableKey {
			table: TableId(42),
		};

		let encoded = key.encode();
		let decoded = TableKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.table, 42);
	}

	#[test]
	fn test_index() {
		let key = IndexKey {
			object: ObjectId::table(42),
			index: IndexId::primary(999_999),
		};

		let encoded = key.encode();
		let decoded = IndexKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.object, ObjectId::table(42));
		assert_eq!(decoded.index, 999_999);
	}

	#[test]
	fn test_row() {
		let key = RowKey {
			storage: StorageId::table(42),
			row: RowNumber(999_999),
		};

		let encoded = key.encode();
		let decoded = RowKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.storage, StorageId::table(42));
		assert_eq!(decoded.row, 999_999);
	}

	#[test]
	fn test_row_sequence() {
		let key = RowSequenceKey {
			storage: StorageId::table(42),
		};

		let encoded = key.encode();
		let decoded = RowSequenceKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.storage, StorageId::table(42));
	}

	#[test]
	fn test_row_sequence_view() {
		// A view owns its row numbering, so decode must keep the view tag intact.
		let key = RowSequenceKey {
			storage: StorageId::view(42),
		};

		let encoded = key.encode();
		let decoded = RowSequenceKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.storage, StorageId::view(42));
	}

	#[test]
	fn test_series_row_does_not_dispatch_to_the_row_decoder() {
		// Under a shared kind byte a series key could masquerade as a row number.
		let key = SeriesRowKey {
			storage: StorageId::series(42),
			variant_tag: None,
			key: 1_706_745_600_000,
			sequence: 3,
		};

		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.storage, StorageId::series(42));
		assert_eq!(decoded.variant_tag, None);
		assert_eq!(decoded.key, 1_706_745_600_000);
		assert_eq!(decoded.sequence, 3);

		assert!(RowKey::decode(&encoded).is_none(), "the row decoder must reject the series layout outright");
	}

	#[test]
	fn test_series_row_and_row_keyspaces_do_not_overlap() {
		// One kind byte for two layouts is what let a 27-byte series key answer to an 18-byte row read.
		let series = SeriesRowKey {
			storage: StorageId::series(1),
			variant_tag: Some(2),
			key: 7,
			sequence: 9,
		}
		.encode();
		let row = RowKey {
			storage: StorageId::table(1),
			row: RowNumber(7),
		}
		.encode();

		assert_ne!(series.as_slice()[0], row.as_slice()[0]);
		assert!(SeriesRowKey::decode(&row).is_none());
	}

	#[test]
	fn test_column_sequence() {
		let key = ColumnSequenceKey {
			object: ObjectId::table(42),
			column: ColumnId(123),
		};

		let encoded = key.encode();
		let decoded = ColumnSequenceKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.object, ObjectId::table(42));
		assert_eq!(decoded.column, 123);
	}

	#[test]
	fn test_transaction_version() {
		let key = TransactionVersionKey {};
		let encoded = key.encode();
		TransactionVersionKey::decode(&encoded).expect("Failed to decode key");
	}

	#[test]
	fn test_operator_state() {
		let key = OperatorStateKey {
			operator: OperatorId(0xCAFEBABE),
			group: GroupId::ROOT,
			keyspace: KeyspaceId::CUSTOM_NOT_CACHED,
			suffix: vec![1, 2, 3],
		};

		let encoded = key.encode();
		let decoded = OperatorStateKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.operator, 0xCAFEBABE);
		assert_eq!(decoded.suffix, vec![1, 2, 3]);
	}

	#[test]
	fn test_sumtype_key() {
		let key = SumTypeKey {
			sumtype: SumTypeId(42),
		};

		let encoded = key.encode();
		let decoded = SumTypeKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.sumtype, 42);
	}

	#[test]
	fn test_relationship() {
		let key = RelationshipKey {
			relationship: RelationshipId(42),
		};

		let encoded = key.encode();
		let decoded = RelationshipKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.relationship, 42);
	}

	#[test]
	fn test_namespace_sumtype_key() {
		let key = NamespaceSumTypeKey {
			namespace: NamespaceId(42),
			sumtype: SumTypeId(999_999),
		};

		let encoded = key.encode();
		let decoded = NamespaceSumTypeKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.namespace, 42);
		assert_eq!(decoded.sumtype, 999_999);
	}
}
