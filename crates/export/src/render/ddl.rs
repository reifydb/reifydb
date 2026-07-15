// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	column::Column,
	dictionary::Dictionary,
	key::{KeySpec, TimestampPrecision},
	namespace::Namespace,
	ringbuffer::RingBuffer,
	segment_tree::SegmentTree,
	series::Series,
	sumtype::{Field, SumType},
	table::Table,
};

use crate::{
	error::ExportError,
	model::NameResolver,
	render::{
		layout::{EnumColumn, LayoutColumn, build_layout},
		typ::{render_column_type, render_value_type},
	},
};

pub fn qualified_name(
	resolver: &NameResolver,
	namespace_id: u64,
	name: &str,
	context: &str,
) -> Result<String, ExportError> {
	let ns = resolver.namespaces.get(&namespace_id).ok_or_else(|| ExportError::UnresolvedReference {
		kind: "namespace",
		id: namespace_id,
		shape: context.to_string(),
	})?;
	Ok(format!("{}::{}", ns, name))
}

fn keyword_prefix(if_not_exists: bool) -> &'static str {
	if if_not_exists {
		" IF NOT EXISTS"
	} else {
		""
	}
}

fn render_column(col: &Column, resolver: &NameResolver, shape: &str) -> Result<String, ExportError> {
	let rendered = render_column_type(&col.constraint, resolver, shape)?;
	let mut type_text = rendered.type_text;
	let mut dictionary = rendered.dictionary;

	if let (true, Some(dict_id)) = (dictionary.is_none(), &col.dictionary_id) {
		let id = dict_id.to_u64();
		let resolved = resolver.dictionary(id).ok_or_else(|| ExportError::UnresolvedReference {
			kind: "dictionary",
			id,
			shape: shape.to_string(),
		})?;
		type_text = render_value_type(&resolved.value_type, shape)?;
		dictionary = Some(resolved.qualified_name.clone());
	}

	let mut properties = Vec::new();
	if let Some(dict) = dictionary {
		properties.push(format!("dictionary: {}", dict));
	}
	if col.auto_increment {
		properties.push("auto_increment".to_string());
	}

	let mut out = format!("{}: {}", col.name, type_text);
	if !properties.is_empty() {
		out.push_str(&format!(" with {{ {} }}", properties.join(", ")));
	}
	Ok(out)
}

fn render_field(field: &Field, resolver: &NameResolver, shape: &str) -> Result<String, ExportError> {
	let rendered = render_column_type(&field.field_type, resolver, shape)?;
	let mut out = format!("{}: {}", field.name, rendered.type_text);
	if let Some(dict) = rendered.dictionary {
		out.push_str(&format!(" with {{ dictionary: {} }}", dict));
	}
	Ok(out)
}

fn render_columns_block(columns: &[Column], resolver: &NameResolver, shape: &str) -> Result<String, ExportError> {
	let mut rendered = Vec::new();
	for column in build_layout(columns, resolver) {
		rendered.push(match column {
			LayoutColumn::Plain(c) => render_column(c, resolver, shape)?,
			LayoutColumn::Enum(e) => render_enum_column(&e, resolver, shape)?,
		});
	}
	Ok(format!("{{ {} }}", rendered.join(", ")))
}

fn render_enum_column(column: &EnumColumn, resolver: &NameResolver, shape: &str) -> Result<String, ExportError> {
	let resolved = resolver.sumtype(column.sumtype_id).ok_or_else(|| ExportError::UnresolvedReference {
		kind: "sumtype",
		id: column.sumtype_id,
		shape: shape.to_string(),
	})?;
	Ok(format!("{}: {}", column.logical_name, resolved.qualified_name))
}

pub fn render_namespace(namespace: &Namespace, if_not_exists: bool) -> String {
	format!("CREATE NAMESPACE{} {};", keyword_prefix(if_not_exists), namespace.name())
}

pub fn render_enum(sumtype: &SumType, resolver: &NameResolver, if_not_exists: bool) -> Result<String, ExportError> {
	let name = qualified_name(resolver, sumtype.namespace.0, &sumtype.name, &sumtype.name)?;
	let mut variants = sumtype.variants.clone();
	variants.sort_by_key(|v| v.tag);

	let mut rendered_variants = Vec::new();
	for variant in &variants {
		if variant.fields.is_empty() {
			rendered_variants.push(variant.name.clone());
		} else {
			let fields: Result<Vec<_>, _> =
				variant.fields.iter().map(|f| render_field(f, resolver, &sumtype.name)).collect();
			rendered_variants.push(format!("{} {{ {} }}", variant.name, fields?.join(", ")));
		}
	}

	Ok(format!("CREATE ENUM{} {} {{ {} }};", keyword_prefix(if_not_exists), name, rendered_variants.join(", ")))
}

pub fn render_dictionary(
	dictionary: &Dictionary,
	resolver: &NameResolver,
	if_not_exists: bool,
) -> Result<String, ExportError> {
	let name = qualified_name(resolver, dictionary.namespace.0, &dictionary.name, &dictionary.name)?;
	let value_type = render_value_type(&dictionary.value_type, &dictionary.name)?;
	let id_type = render_value_type(&dictionary.id_type, &dictionary.name)?;
	Ok(format!("CREATE DICTIONARY{} {} FOR {} AS {};", keyword_prefix(if_not_exists), name, value_type, id_type))
}

pub fn render_table(table: &Table, resolver: &NameResolver, if_not_exists: bool) -> Result<String, ExportError> {
	let name = qualified_name(resolver, table.namespace.0, &table.name, &table.name)?;
	let columns = render_columns_block(&table.columns, resolver, &table.name)?;
	let with = if table.partition_by.is_empty() {
		String::new()
	} else {
		format!(" WITH {{ partition: {{ by: {{ {} }} }} }}", table.partition_by.join(", "))
	};
	Ok(format!("CREATE TABLE{} {} {}{};", keyword_prefix(if_not_exists), name, columns, with))
}

pub fn render_ringbuffer(ringbuffer: &RingBuffer, resolver: &NameResolver) -> Result<String, ExportError> {
	let name = qualified_name(resolver, ringbuffer.namespace.0, &ringbuffer.name, &ringbuffer.name)?;
	let columns = render_columns_block(&ringbuffer.columns, resolver, &ringbuffer.name)?;

	let mut with = format!("capacity: {}", ringbuffer.capacity);
	if !ringbuffer.partition_by.is_empty() {
		with.push_str(&format!(", partition: {{ by: {{ {} }} }}", ringbuffer.partition_by.join(", ")));
	}

	Ok(format!("CREATE RINGBUFFER {} {} WITH {{ {} }};", name, columns, with))
}

pub fn render_series(series: &Series, resolver: &NameResolver) -> Result<String, ExportError> {
	let name = qualified_name(resolver, series.namespace.0, &series.name, &series.name)?;
	let columns = render_columns_block(&series.columns, resolver, &series.name)?;

	let mut with = format!("key: {}", series.key.column());

	if let Some(tag_id) = &series.tag {
		let id = tag_id.to_u64();
		let resolved = resolver.sumtype(id).ok_or_else(|| ExportError::UnresolvedReference {
			kind: "sumtype",
			id,
			shape: series.name.clone(),
		})?;
		with.push_str(&format!(", tag: {}", resolved.qualified_name));
	}

	if let KeySpec::DateTime {
		precision,
		..
	} = &series.key
	{
		with.push_str(&format!(", precision: {}", render_precision(precision)));
	}

	if !series.partition_by.is_empty() {
		with.push_str(&format!(", partition: {{ by: {{ {} }} }}", series.partition_by.join(", ")));
	}

	Ok(format!("CREATE SERIES {} {} WITH {{ {} }};", name, columns, with))
}

pub fn render_segment_tree(segment_tree: &SegmentTree, resolver: &NameResolver) -> Result<String, ExportError> {
	let name = qualified_name(resolver, segment_tree.namespace.0, &segment_tree.name, &segment_tree.name)?;
	let columns = render_columns_block(&segment_tree.columns, resolver, &segment_tree.name)?;

	let mut with = format!("key: {}", segment_tree.key.column());

	if let KeySpec::DateTime {
		precision,
		..
	} = &segment_tree.key
	{
		with.push_str(&format!(", precision: {}", render_precision(precision)));
	}

	with.push_str(&format!(", aggregates: {{ {} }}", segment_tree.render_aggregates()));

	if !segment_tree.partition_by.is_empty() {
		with.push_str(&format!(", partition: {{ by: {{ {} }} }}", segment_tree.partition_by.join(", ")));
	}

	Ok(format!("CREATE SEGMENTTREE {} {} WITH {{ {} }};", name, columns, with))
}

fn render_precision(precision: &TimestampPrecision) -> &'static str {
	match precision {
		TimestampPrecision::Second => "second",
		TimestampPrecision::Millisecond => "millisecond",
		TimestampPrecision::Microsecond => "microsecond",
		TimestampPrecision::Nanosecond => "nanosecond",
	}
}
