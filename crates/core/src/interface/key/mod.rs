// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::util::encoding::keycode;
use crate::EncodedKey;
pub use column::ColumnKey;
pub use column_policy::ColumnPolicyKey;
pub use kind::KeyKind;
pub use schema::SchemaKey;
pub use schema_table::SchemaTableKey;
pub use system_sequence::SystemSequenceKey;
pub use system_version::{SystemVersion, SystemVersionKey};
pub use table::TableKey;
pub use table_column::TableColumnKey;
pub use table_row::TableRowKey;
pub use table_row_sequence::TableRowSequenceKey;

mod column;
mod column_policy;
mod kind;
mod schema;
mod schema_table;
mod system_sequence;
mod system_version;
mod table;
mod table_column;
mod table_row;
mod table_row_sequence;

#[derive(Debug)]
pub enum Key {
    Column(ColumnKey),
    ColumnPolicy(ColumnPolicyKey),
    Schema(SchemaKey),
    SchemaTable(SchemaTableKey),
    SystemSequence(SystemSequenceKey),
    Table(TableKey),
    TableColumn(TableColumnKey),
    TableRow(TableRowKey),
    TableRowSequence(TableRowSequenceKey),
    SystemVersion(SystemVersionKey),
}

impl Key {
    pub fn encode(&self) -> EncodedKey {
        match &self {
            Key::Column(key) => key.encode(),
            Key::ColumnPolicy(key) => key.encode(),
            Key::Schema(key) => key.encode(),
            Key::SchemaTable(key) => key.encode(),
            Key::Table(key) => key.encode(),
            Key::TableColumn(key) => key.encode(),
            Key::TableRow(key) => key.encode(),
            Key::TableRowSequence(key) => key.encode(),
            Key::SystemSequence(key) => key.encode(),
            Key::SystemVersion(key) => key.encode(),
        }
    }
}

pub trait EncodableKey {
    const KIND: KeyKind;

    fn encode(&self) -> EncodedKey;

    fn decode(version: u8, payload: &[u8]) -> Option<Self>
    where
        Self: Sized;
}

impl Key {
    pub fn decode(key: &EncodedKey) -> Option<Self> {
        if key.len() < 2 {
            return None;
        }

        let version = keycode::deserialize(&key[0..1]).ok()?;
        let kind: KeyKind = keycode::deserialize(&key[1..2]).ok()?;
        let payload = &key[2..];

        match kind {
            KeyKind::Column => ColumnKey::decode(version, payload).map(Self::Column),
            KeyKind::ColumnPolicy => {
                ColumnPolicyKey::decode(version, payload).map(Self::ColumnPolicy)
            }
            KeyKind::Schema => SchemaKey::decode(version, payload).map(Self::Schema),
            KeyKind::SchemaTable => SchemaTableKey::decode(version, payload).map(Self::SchemaTable),
            KeyKind::Table => TableKey::decode(version, payload).map(Self::Table),
            KeyKind::TableColumn => TableColumnKey::decode(version, payload).map(Self::TableColumn),
            KeyKind::TableRow => TableRowKey::decode(version, payload).map(Self::TableRow),
            KeyKind::TableRowSequence => {
                TableRowSequenceKey::decode(version, payload).map(Self::TableRowSequence)
            }
            KeyKind::SystemSequence => {
                SystemSequenceKey::decode(version, payload).map(Self::SystemSequence)
            }
            KeyKind::SystemVersion => {
                SystemVersionKey::decode(version, payload).map(Self::SystemVersion)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnPolicyKey;
    use super::TableRowSequenceKey;
    use super::{
        ColumnKey, Key, SchemaKey, SchemaTableKey, SystemSequenceKey, TableColumnKey, TableKey,
        TableRowKey,
    };
    use crate::interface::catalog::{
        ColumnId, ColumnPolicyId, SchemaId, SystemSequenceId, TableId,
    };
    use crate::RowId;

    #[test]
    fn test_column() {
        let key = Key::Column(ColumnKey { column: ColumnId(42) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

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
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

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
        let key = Key::Schema(SchemaKey { schema: SchemaId(42) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::Schema(decoded_inner) => {
                assert_eq!(decoded_inner.schema, 42);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_schema_table() {
        let key =
            Key::SchemaTable(SchemaTableKey { schema: SchemaId(42), table: TableId(999_999) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

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
        let key = Key::SystemSequence(SystemSequenceKey { sequence: SystemSequenceId(42) });

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
        let key = Key::Table(TableKey { table: TableId(42) });

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
    fn test_table_column() {
        let key =
            Key::TableColumn(TableColumnKey { table: TableId(42), column: ColumnId(999_999) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::TableColumn(decoded_inner) => {
                assert_eq!(decoded_inner.table, 42);
                assert_eq!(decoded_inner.column, 999_999);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_table_row() {
        let key = Key::TableRow(TableRowKey { table: TableId(42), row: RowId(999_999) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

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
        let key = Key::TableRowSequence(TableRowSequenceKey { table: TableId(42) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::TableRowSequence(decoded_inner) => {
                assert_eq!(decoded_inner.table, 42);
            }
            _ => unreachable!(),
        }
    }
}
