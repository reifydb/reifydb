// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;

use super::{RingBufferDef, TableDef, ViewDef};
use crate::value::row::{EncodedRowLayout, EncodedRowNamedLayout};

pub trait GetEncodedRowLayout {
	fn get_layout(&self) -> EncodedRowLayout;
}

pub trait GetEncodedRowNamedLayout {
	fn get_named_layout(&self) -> EncodedRowNamedLayout;
}

impl GetEncodedRowLayout for TableDef {
	fn get_layout(&self) -> EncodedRowLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedRowLayout::new(&types)
	}
}

impl GetEncodedRowLayout for ViewDef {
	fn get_layout(&self) -> EncodedRowLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedRowLayout::new(&types)
	}
}

impl GetEncodedRowLayout for RingBufferDef {
	fn get_layout(&self) -> EncodedRowLayout {
		let types: Vec<_> = self.columns.iter().map(|col| col.constraint.get_type()).collect();
		EncodedRowLayout::new(&types)
	}
}

impl GetEncodedRowNamedLayout for TableDef {
	fn get_named_layout(&self) -> EncodedRowNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedRowNamedLayout::new(fields)
	}
}

impl GetEncodedRowNamedLayout for ViewDef {
	fn get_named_layout(&self) -> EncodedRowNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedRowNamedLayout::new(fields)
	}
}

impl GetEncodedRowNamedLayout for RingBufferDef {
	fn get_named_layout(&self) -> EncodedRowNamedLayout {
		let fields: Vec<(String, Type)> =
			self.columns.iter().map(|col| (col.name.clone(), col.constraint.get_type())).collect();
		EncodedRowNamedLayout::new(fields)
	}
}
