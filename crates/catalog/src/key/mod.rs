// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use column::ColumnKey;
use reifydb_core::EncodedKey;
pub use schema::SchemaKey;
pub use schema_table::SchemaTableKey;
pub use sequence::SequenceKey;
pub use sequence_value::SequenceValueKey;
pub use table::TableKey;
pub use table_column::TableColumnKey;
pub use table_row::TableRowKey;

mod column;
mod schema;
mod schema_table;
mod sequence;
mod sequence_value;
mod table;
mod table_column;
mod table_row;

#[derive(Debug)]
pub enum Key {
    Schema(SchemaKey),
    Table(TableKey),
    TableRow(TableRowKey),
    SchemaTable(SchemaTableKey),
    Sequence(SequenceKey),
    SequenceValue(SequenceValueKey),
    Column(ColumnKey),
    TableColumn(TableColumnKey),
}

impl Key {
    pub fn encode(&self) -> EncodedKey {
        match &self {
            Key::Schema(key) => key.encode(),
            Key::Table(key) => key.encode(),
            Key::TableRow(key) => key.encode(),
            Key::SchemaTable(key) => key.encode(),
            Key::Sequence(key) => key.encode(),
            Key::SequenceValue(key) => key.encode(),
            Key::Column(key) => key.encode(),
            Key::TableColumn(key) => key.encode(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Schema = 0x01,
    Table = 0x02,
    TableRow = 0x03,
    SchemaTableLink = 0x04,
    Sequence = 0x05,
    SequenceValue = 0x06,
    Column = 0x07,
    TableColumnLink = 0x08,
}

impl TryFrom<u8> for KeyKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Schema),
            0x02 => Ok(Self::Table),
            0x03 => Ok(Self::TableRow),
            0x04 => Ok(Self::SchemaTableLink),
            0x05 => Ok(Self::Sequence),
            0x06 => Ok(Self::SequenceValue),
            0x07 => Ok(Self::Column),
            0x08 => Ok(Self::TableColumnLink),
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
            KeyKind::Schema => SchemaKey::decode(version, payload).map(Self::Schema),
            KeyKind::Table => TableKey::decode(version, payload).map(Self::Table),
            KeyKind::TableRow => TableRowKey::decode(version, payload).map(Self::TableRow),
            KeyKind::SchemaTableLink => {
                SchemaTableKey::decode(version, payload).map(Self::SchemaTable)
            }
            KeyKind::Sequence => SequenceKey::decode(version, payload).map(Self::Sequence),
            KeyKind::SequenceValue => {
                SequenceValueKey::decode(version, payload).map(Self::SequenceValue)
            }
            KeyKind::Column => ColumnKey::decode(version, payload).map(Self::Column),
            KeyKind::TableColumnLink => {
                TableColumnKey::decode(version, payload).map(Self::TableColumn)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::ColumnId;
    use crate::key::{
        ColumnKey, Key, SchemaKey, SchemaTableKey, SequenceKey, TableColumnKey, TableKey,
        TableRowKey,
    };
    use crate::schema::SchemaId;
    use crate::sequence::SequenceId;
    use crate::table::TableId;
    use reifydb_core::catalog::RowId;

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
    fn test_schema_table_link() {
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
    fn test_sequence() {
        let key = Key::Sequence(SequenceKey { sequence: SequenceId(42) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::Sequence(decoded_inner) => {
                assert_eq!(decoded_inner.sequence, 42);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_sequence_value() {
        let key = Key::Sequence(SequenceKey { sequence: SequenceId(42) });

        let encoded = key.encode();
        let decoded = Key::decode(&encoded).expect("Failed to decode key");

        match decoded {
            Key::Sequence(decoded_inner) => {
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
    fn test_table_column_link() {
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
}
