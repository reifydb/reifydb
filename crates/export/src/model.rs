// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::{
	dictionary::Dictionary, namespace::Namespace, ringbuffer::RingBuffer, series::Series, sumtype::SumType,
	table::Table,
};
use reifydb_value::value::{Value, value_type::ValueType};

pub struct ShapeRows {
	pub columns: Vec<String>,
	pub rows: Vec<Vec<Value>>,
}

pub struct TableExport {
	pub table: Table,
	pub rows: Option<ShapeRows>,
}

pub struct RingBufferExport {
	pub ringbuffer: RingBuffer,
	pub rows: Option<ShapeRows>,
}

pub struct SeriesExport {
	pub series: Series,
	pub rows: Option<ShapeRows>,
}

pub struct ExportModel {
	pub namespaces: Vec<Namespace>,
	pub sumtypes: Vec<SumType>,
	pub dictionaries: Vec<Dictionary>,
	pub tables: Vec<TableExport>,
	pub ringbuffers: Vec<RingBufferExport>,
	pub series: Vec<SeriesExport>,
	pub resolver: NameResolver,
}

pub struct NameResolver {
	pub namespaces: HashMap<u64, String>,
	pub dictionaries: HashMap<u64, ResolvedDictionary>,
	pub sumtypes: HashMap<u64, ResolvedSumType>,
}

pub struct ResolvedDictionary {
	pub qualified_name: String,
	pub value_type: ValueType,
}

pub struct ResolvedSumType {
	pub qualified_name: String,
	pub variants: Vec<ResolvedVariant>,
}

pub struct ResolvedVariant {
	pub tag: u8,
	pub name: String,
	pub fields: Vec<String>,
}

impl NameResolver {
	pub fn empty() -> Self {
		Self {
			namespaces: HashMap::new(),
			dictionaries: HashMap::new(),
			sumtypes: HashMap::new(),
		}
	}

	pub fn dictionary(&self, id: u64) -> Option<&ResolvedDictionary> {
		self.dictionaries.get(&id)
	}

	pub fn sumtype(&self, id: u64) -> Option<&ResolvedSumType> {
		self.sumtypes.get(&id)
	}

	pub fn sumtype_variant(&self, id: u64, tag: u8) -> Option<&ResolvedVariant> {
		self.sumtypes.get(&id).and_then(|st| st.variants.iter().find(|v| v.tag == tag))
	}

	pub fn sumtype_variant_name(&self, id: u64, tag: u8) -> Option<&str> {
		self.sumtype_variant(id, tag).map(|v| v.name.as_str())
	}
}
