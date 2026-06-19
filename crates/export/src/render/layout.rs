// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_core::interface::catalog::column::Column;
use reifydb_value::value::constraint::Constraint;

use crate::model::NameResolver;

pub enum LayoutColumn<'a> {
	Plain(&'a Column),
	Enum(EnumColumn),
}

pub struct EnumColumn {
	pub logical_name: String,
	pub sumtype_id: u64,
	pub tag_column: String,
	pub variants: Vec<EnumVariant>,
}

pub struct EnumVariant {
	pub tag: u8,
	pub name: String,
	pub fields: Vec<(String, String)>,
}

pub fn build_layout<'a>(columns: &'a [Column], resolver: &NameResolver) -> Vec<LayoutColumn<'a>> {
	let mut enum_at: HashMap<usize, EnumColumn> = HashMap::new();
	let mut absorbed: HashSet<String> = HashSet::new();

	for (idx, col) in columns.iter().enumerate() {
		let Some(Constraint::SumType(id)) = col.constraint.constraint() else {
			continue;
		};
		let sumtype_id = id.to_u64();
		let logical_name = col.name.strip_suffix("_tag").unwrap_or(&col.name).to_string();

		let mut variants = Vec::new();
		if let Some(resolved) = resolver.sumtype(sumtype_id) {
			for variant in &resolved.variants {
				let fields: Vec<(String, String)> = variant
					.fields
					.iter()
					.map(|field| {
						(field.clone(), format!("{logical_name}_{}_{}", variant.name, field))
					})
					.collect();
				for (_, physical) in &fields {
					absorbed.insert(physical.clone());
				}
				variants.push(EnumVariant {
					tag: variant.tag,
					name: variant.name.clone(),
					fields,
				});
			}
		}

		enum_at.insert(
			idx,
			EnumColumn {
				logical_name,
				sumtype_id,
				tag_column: col.name.clone(),
				variants,
			},
		);
	}

	let mut layout = Vec::with_capacity(columns.len());
	for (idx, col) in columns.iter().enumerate() {
		if let Some(enum_col) = enum_at.remove(&idx) {
			layout.push(LayoutColumn::Enum(enum_col));
		} else if !absorbed.contains(&col.name) {
			layout.push(LayoutColumn::Plain(col));
		}
	}
	layout
}
