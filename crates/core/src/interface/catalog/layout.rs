// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{ringbuffer::RingBufferDef, table::TableDef, view::ViewDef};
use crate::{encoded::layout::EncodedValuesLayout, schema::Schema};

pub trait GetEncodedRowLayout {
	fn get_layout(&self) -> EncodedValuesLayout;
}

impl GetEncodedRowLayout for TableDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let schema = Schema::from(&self.columns);
		EncodedValuesLayout::from_schema(schema)
	}
}

impl GetEncodedRowLayout for ViewDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let schema = Schema::from(&self.columns);
		EncodedValuesLayout::from_schema(schema)
	}
}

impl GetEncodedRowLayout for RingBufferDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let schema = Schema::from(&self.columns);
		EncodedValuesLayout::from_schema(schema)
	}
}
