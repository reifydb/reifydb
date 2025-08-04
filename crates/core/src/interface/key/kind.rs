// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
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
    SystemVersion = 0x0A,
    TransactionVersion = 0x0B,
    TableIndex = 0x0C,
    TableIndexEntry = 0x0D,
}

impl From<KeyKind> for u8 {
    fn from(kind: KeyKind) -> Self {
        kind as u8
    }
}
impl TryFrom<u8> for KeyKind {
    type Error = serde::de::value::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Schema),
            0x02 => Ok(Self::Table),
            0x03 => Ok(Self::TableRow),
            0x04 => Ok(Self::SchemaTable),
            0x05 => Ok(Self::SystemSequence),
            0x06 => Ok(Self::Column),
            0x07 => Ok(Self::TableColumn),
            0x08 => Ok(Self::TableRowSequence),
            0x09 => Ok(Self::ColumnPolicy),
            0x0A => Ok(Self::SystemVersion),
            0x0B => Ok(Self::TransactionVersion),
            0x0C => Ok(Self::TableIndex),
            0x0D => Ok(Self::TableIndexEntry),
            _ => Err(serde::de::Error::custom(format!("Invalid KeyKind value: {value:#04x}"))),
        }
    }
}
