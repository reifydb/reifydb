// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ddl;
pub mod dml;
pub mod layout;
pub mod typ;
pub mod value;

use crate::{
	error::ExportError,
	model::ExportModel,
	options::ExportOptions,
	render::{dml::render_inserts, layout::build_layout},
};

pub fn render_script(model: &ExportModel, options: &ExportOptions) -> Result<String, ExportError> {
	let mut out = String::new();
	out.push_str("# ReifyDB logical export\n\n");

	if options.includes_schema() {
		for namespace in &model.namespaces {
			out.push_str(&ddl::render_namespace(namespace, options.if_not_exists));
			out.push('\n');
		}
		if !model.namespaces.is_empty() {
			out.push('\n');
		}
		for sumtype in &model.sumtypes {
			out.push_str(&ddl::render_enum(sumtype, &model.resolver, options.if_not_exists)?);
			out.push('\n');
		}
		for dictionary in &model.dictionaries {
			out.push_str(&ddl::render_dictionary(dictionary, &model.resolver, options.if_not_exists)?);
			out.push('\n');
		}
		for table in &model.tables {
			out.push_str(&ddl::render_table(&table.table, &model.resolver, options.if_not_exists)?);
			out.push('\n');
		}
		for ringbuffer in &model.ringbuffers {
			out.push_str(&ddl::render_ringbuffer(&ringbuffer.ringbuffer, &model.resolver)?);
			out.push('\n');
		}
		for series in &model.series {
			out.push_str(&ddl::render_series(&series.series, &model.resolver)?);
			out.push('\n');
		}
		for segment_tree in &model.segment_trees {
			out.push_str(&ddl::render_segment_tree(&segment_tree.segment_tree, &model.resolver)?);
			out.push('\n');
		}
		out.push('\n');
	}

	if options.includes_data() {
		for table in &model.tables {
			if let Some(rows) = &table.rows {
				let name = ddl::qualified_name(
					&model.resolver,
					table.table.namespace.0,
					&table.table.name,
					&table.table.name,
				)?;
				let layout = build_layout(&table.table.columns, &model.resolver);
				out.push_str(&render_inserts(
					&name,
					rows,
					options.insert_batch_size,
					&layout,
					&model.resolver,
				)?);
			}
		}
		for ringbuffer in &model.ringbuffers {
			if let Some(rows) = &ringbuffer.rows {
				let name = ddl::qualified_name(
					&model.resolver,
					ringbuffer.ringbuffer.namespace.0,
					&ringbuffer.ringbuffer.name,
					&ringbuffer.ringbuffer.name,
				)?;
				let layout = build_layout(&ringbuffer.ringbuffer.columns, &model.resolver);
				out.push_str(&render_inserts(
					&name,
					rows,
					options.insert_batch_size,
					&layout,
					&model.resolver,
				)?);
			}
		}
		for series in &model.series {
			if let Some(rows) = &series.rows {
				let name = ddl::qualified_name(
					&model.resolver,
					series.series.namespace.0,
					&series.series.name,
					&series.series.name,
				)?;
				let layout = build_layout(&series.series.columns, &model.resolver);
				out.push_str(&render_inserts(
					&name,
					rows,
					options.insert_batch_size,
					&layout,
					&model.resolver,
				)?);
			}
		}
	}

	Ok(out)
}
