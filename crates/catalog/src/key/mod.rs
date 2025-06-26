// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::ColumnKey;
pub use column_policy::ColumnPolicyKey;
use reifydb_core::EncodedKey;
pub use schema::SchemaKey;
pub use schema_table::SchemaTableKey;
pub use system_sequence::SystemSequenceKey;
pub use table::TableKey;
pub use table_column::TableColumnKey;
pub use table_row::TableRowKey;
pub use table_row_sequence::TableRowSequenceKey;

mod column;
mod column_policy;
mod schema;
mod schema_table;
mod system_sequence;
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
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Schema = 0x01,
    Table = 0x02,
    TableRow = 0x03,
    SchemaTable = 0x04,
    SystemSequence = 0x05,
    Column = 0x06,
    TableColumn = 0x07,
    TableRowSequence = 0x08,
    ColumnPolicy = 0x09,
}

impl TryFrom<u8> for KeyKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x06 => Ok(Self::Column),
            0x09 => Ok(Self::ColumnPolicy),
            0x01 => Ok(Self::Schema),
            0x04 => Ok(Self::SchemaTable),
            0x08 => Ok(Self::TableRowSequence),
            0x05 => Ok(Self::SystemSequence),
            0x02 => Ok(Self::Table),
            0x07 => Ok(Self::TableColumn),
            0x03 => Ok(Self::TableRow),
            _ => Err(()),
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
        let version = *key.get(0)?;
        let kind = *key.get(1)?;
        let payload = &key[2..];

        match KeyKind::try_from(kind).ok()? {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnId;
    use crate::column_policy::ColumnPolicyId;
    use crate::key::column_policy::ColumnPolicyKey;
    use crate::key::table_row_sequence::TableRowSequenceKey;
    use crate::key::{
        ColumnKey, Key, SchemaKey, SchemaTableKey, SystemSequenceKey, TableColumnKey, TableKey,
        TableRowKey,
    };
    use crate::row::RowId;
    use crate::schema::SchemaId;
    use crate::sequence::SystemSequenceId;
    use crate::table::TableId;

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
