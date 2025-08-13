// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::LazyLock;

use reifydb_core::{Type, row::EncodedRowLayout};

pub(crate) static CDC_EVENT_LAYOUT: LazyLock<EncodedRowLayout> =
	LazyLock::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // version
			Type::Uint2, // sequence
			Type::Uint8, // timestamp
			Type::Uint1, /* change_type (0=Insert, 1=Update,
			              * 2=Delete) */
			Type::Blob, // key
			Type::Blob, // before (optional, undefined for Insert)
			Type::Blob, // after (optional, undefined for Delete)
		])
	});

pub(crate) const CDC_VERSION_FIELD: usize = 0;
pub(crate) const CDC_SEQUENCE_FIELD: usize = 1;
pub(crate) const CDC_TIMESTAMP_FIELD: usize = 2;
pub(crate) const CDC_CHANGE_TYPE_FIELD: usize = 3;
pub(crate) const CDC_KEY_FIELD: usize = 4;
pub(crate) const CDC_BEFORE_FIELD: usize = 5;
pub(crate) const CDC_AFTER_FIELD: usize = 6;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ChangeType {
	Insert = 0,
	Update = 1,
	Delete = 2,
}

impl From<u8> for ChangeType {
	fn from(value: u8) -> Self {
		match value {
			0 => ChangeType::Insert,
			1 => ChangeType::Update,
			2 => ChangeType::Delete,
			_ => panic!("Invalid change type: {}", value),
		}
	}
}
