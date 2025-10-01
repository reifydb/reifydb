// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::LazyLock;

use reifydb_core::value::row::EncodedRowLayout;
use reifydb_type::Type;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ChangeType {
	Insert = 1,
	Update = 2,
	Delete = 3,
}

impl From<u8> for ChangeType {
	fn from(value: u8) -> Self {
		match value {
			1 => ChangeType::Insert,
			2 => ChangeType::Update,
			3 => ChangeType::Delete,
			_ => panic!("Invalid change type: {}", value),
		}
	}
}

// Layout for efficient transaction storage (shared metadata + packed changes)
pub(crate) static CDC_TRANSACTION_LAYOUT: LazyLock<EncodedRowLayout> = LazyLock::new(|| {
	EncodedRowLayout::new(&[
		Type::Uint8, // version
		Type::Uint8, // timestamp
		Type::Blob,  // transaction
		Type::Blob,  // packed changes array
	])
});

pub(crate) const CDC_TX_VERSION_FIELD: usize = 0;
pub(crate) const CDC_TX_TIMESTAMP_FIELD: usize = 1;
pub(crate) const CDC_TX_TRANSACTION_FIELD: usize = 2;
pub(crate) const CDC_TX_CHANGES_FIELD: usize = 3;

// Layout for individual changes (without metadata)
pub(crate) static CDC_CHANGE_LAYOUT: LazyLock<EncodedRowLayout> = LazyLock::new(|| {
	EncodedRowLayout::new(&[
		Type::Uint1, // change_type (1=Insert, 2=Update, 3=Delete)
		Type::Blob,  // key
		Type::Blob,  // pre
		Type::Blob,  // post
	])
});

pub(crate) const CDC_COMPACT_CHANGE_TYPE_FIELD: usize = 0;
pub(crate) const CDC_COMPACT_CHANGE_KEY_FIELD: usize = 1;
pub(crate) const CDC_COMPACT_CHANGE_PRE_FIELD: usize = 2;
pub(crate) const CDC_COMPACT_CHANGE_POST_FIELD: usize = 3;
