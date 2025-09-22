// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::value::row::EncodedRowLayout;
use reifydb_type::Type;

pub(crate) mod ring_buffer {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const CAPACITY: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Uint8, // namespace id
			Type::Utf8,  // name
			Type::Uint8, // capacity
			Type::Uint8, // primary_key (0 if none)
		])
	});
}

pub(crate) mod ring_buffer_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id
			Type::Utf8,  // name
		])
	});
}

pub(crate) mod ring_buffer_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const CAPACITY: usize = 1;
	pub(crate) const HEAD: usize = 2;
	pub(crate) const TAIL: usize = 3;
	pub(crate) const COUNT: usize = 4;

	pub(crate) static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // ring_buffer_id
			Type::Uint8, // capacity
			Type::Uint8, // head position
			Type::Uint8, // tail position
			Type::Uint8, // current count
		])
	});
}
