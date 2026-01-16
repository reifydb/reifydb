// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

use super::{ringbuffer::RingBufferDef, table::TableDef, view::ViewDef};
use crate::encoded::{layout::EncodedValuesLayout, named::EncodedValuesNamedLayout};

pub trait GetEncodedRowLayout {
	fn get_layout(&self) -> EncodedValuesLayout;
}

pub trait GetEncodedRowNamedLayout {
	fn get_named_layout(&self) -> EncodedValuesNamedLayout;
}

impl GetEncodedRowLayout for TableDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedValuesLayout::new(&types)
	}
}

impl GetEncodedRowLayout for ViewDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedValuesLayout::new(&types)
	}
}

impl GetEncodedRowLayout for RingBufferDef {
	fn get_layout(&self) -> EncodedValuesLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedValuesLayout::new(&types)
	}
}

impl GetEncodedRowNamedLayout for TableDef {
	fn get_named_layout(&self) -> EncodedValuesNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedValuesNamedLayout::new(fields)
	}
}

impl GetEncodedRowNamedLayout for ViewDef {
	fn get_named_layout(&self) -> EncodedValuesNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedValuesNamedLayout::new(fields)
	}
}

impl GetEncodedRowNamedLayout for RingBufferDef {
	fn get_named_layout(&self) -> EncodedValuesNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedValuesNamedLayout::new(fields)
	}
}
