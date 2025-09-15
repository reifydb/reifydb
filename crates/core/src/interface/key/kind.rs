// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Error;
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum KeyKind {
	Namespace = 0x01,
	Table = 0x02,
	Row = 0x03,
	NamespaceTable = 0x04,
	SystemSequence = 0x05,
	Columns = 0x06,
	Column = 0x07,
	RowSequence = 0x08,
	ColumnPolicy = 0x09,
	SystemVersion = 0x0A,
	TransactionVersion = 0x0B,
	Index = 0x0C,
	IndexEntry = 0x0D,
	ColumnSequence = 0x0E,
	CdcConsumer = 0x0F,
	View = 0x10,
	NamespaceView = 0x11,
	PrimaryKey = 0x12,
	FlowNodeState = 0x13,
	RingBuffer = 0x14,
	NamespaceRingBuffer = 0x15,
	RingBufferMetadata = 0x16,
}

impl From<KeyKind> for u8 {
	fn from(kind: KeyKind) -> Self {
		kind as u8
	}
}
impl TryFrom<u8> for KeyKind {
	type Error = Error;

	fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
		match value {
			0x01 => Ok(Self::Namespace),
			0x02 => Ok(Self::Table),
			0x03 => Ok(Self::Row),
			0x04 => Ok(Self::NamespaceTable),
			0x05 => Ok(Self::SystemSequence),
			0x06 => Ok(Self::Columns),
			0x07 => Ok(Self::Column),
			0x08 => Ok(Self::RowSequence),
			0x09 => Ok(Self::ColumnPolicy),
			0x0A => Ok(Self::SystemVersion),
			0x0B => Ok(Self::TransactionVersion),
			0x0C => Ok(Self::Index),
			0x0D => Ok(Self::IndexEntry),
			0x0E => Ok(Self::ColumnSequence),
			0x0F => Ok(Self::CdcConsumer),
			0x10 => Ok(Self::View),
			0x11 => Ok(Self::NamespaceView),
			0x12 => Ok(Self::PrimaryKey),
			0x13 => Ok(Self::FlowNodeState),
			0x14 => Ok(Self::RingBuffer),
			0x15 => Ok(Self::NamespaceRingBuffer),
			0x16 => Ok(Self::RingBufferMetadata),
			_ => Err(serde::de::Error::custom(format!(
				"Invalid KeyKind value: {value:#04x}"
			))),
		}
	}
}
