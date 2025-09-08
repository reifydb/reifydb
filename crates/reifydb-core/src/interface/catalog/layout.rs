// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{TableDef, ViewDef};
use crate::row::EncodedRowLayout;

pub trait GetEncodedRowLayout {
	fn get_layout(&self) -> EncodedRowLayout;
}

impl GetEncodedRowLayout for TableDef {
	fn get_layout(&self) -> EncodedRowLayout {
		let types: Vec<_> = self
			.columns
			.iter()
			.map(|col| col.constraint.ty())
			.collect();
		EncodedRowLayout::new(&types)
	}
}

impl GetEncodedRowLayout for ViewDef {
	fn get_layout(&self) -> EncodedRowLayout {
		let types: Vec<_> = self
			.columns
			.iter()
			.map(|col| col.constraint.ty())
			.collect();
		EncodedRowLayout::new(&types)
	}
}
