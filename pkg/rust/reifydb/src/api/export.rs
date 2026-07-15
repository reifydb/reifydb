// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, fs, path::Path};

use reifydb_core::{
	interface::catalog::{
		column::Column,
		dictionary::Dictionary,
		id::NamespaceId,
		namespace::Namespace,
		ringbuffer::RingBuffer,
		segment_tree::SegmentTree,
		series::Series,
		sumtype::{SumType, SumTypeKind},
		table::Table,
	},
	internal,
};
use reifydb_export::{
	model::{
		ExportModel, NameResolver, ResolvedDictionary, ResolvedSumType, ResolvedVariant, RingBufferExport,
		SegmentTreeExport, SeriesExport, ShapeRows, TableExport,
	},
	options::{ExportOptions, ExportSelection, ShapeKind},
	render::render_script,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	error::Error,
	params::Params,
	value::{Value, constraint::Constraint, identity::IdentityId},
};

use crate::Database;

impl Database {
	pub fn export(&self, options: &ExportOptions) -> Result<String> {
		let model = self.build_export_model(options)?;
		render_script(&model, options).map_err(|e| Error(Box::new(internal!("export rendering failed: {}", e))))
	}

	pub fn export_to_file(&self, options: &ExportOptions, path: impl AsRef<Path>) -> Result<()> {
		let script = self.export(options)?;
		fs::write(path, script).map_err(|e| Error(Box::new(internal!("failed to write export file: {}", e))))
	}

	pub fn import(&self, rql: &str) -> Result<()> {
		self.admin_as_root(rql, Params::None).map(|_| ())
	}

	pub fn import_from_file(&self, path: impl AsRef<Path>) -> Result<()> {
		let rql = fs::read_to_string(path)
			.map_err(|e| Error(Box::new(internal!("failed to read import file: {}", e))))?;
		self.import(&rql)
	}

	fn build_export_model(&self, options: &ExportOptions) -> Result<ExportModel> {
		let catalog = self.catalog();

		let (
			user_namespaces,
			all_dictionaries,
			all_sumtypes,
			all_tables,
			all_ringbuffers,
			all_series,
			all_segment_trees,
		) = {
			let mut qt = self.engine().begin_query(IdentityId::root())?;
			let mut txn = Transaction::Query(&mut qt);

			let user_namespaces: Vec<Namespace> =
				catalog.list_namespaces_all(&mut txn)?.into_iter().filter(is_user_namespace).collect();
			let user_ids: HashSet<u64> = user_namespaces.iter().map(|ns| ns.id().0).collect();

			let all_dictionaries: Vec<Dictionary> = catalog
				.list_all_dictionaries(&mut txn)?
				.into_iter()
				.filter(|d| user_ids.contains(&d.namespace.0))
				.collect();

			let mut all_sumtypes: Vec<SumType> = Vec::new();
			for ns in &user_namespaces {
				all_sumtypes.extend(catalog.list_sumtypes(&mut txn, ns.id())?);
			}

			let all_tables: Vec<Table> = catalog
				.list_tables_all(&mut txn)?
				.into_iter()
				.filter(|t| !t.underlying && user_ids.contains(&t.namespace.0))
				.collect();
			let all_ringbuffers: Vec<RingBuffer> = catalog
				.list_ringbuffers_all(&mut txn)?
				.into_iter()
				.filter(|r| !r.underlying && user_ids.contains(&r.namespace.0))
				.collect();
			let all_series: Vec<Series> = catalog
				.list_series_all(&mut txn)?
				.into_iter()
				.filter(|s| !s.underlying && user_ids.contains(&s.namespace.0))
				.collect();
			let all_segment_trees: Vec<SegmentTree> = catalog
				.list_segment_tree_all(&mut txn)?
				.into_iter()
				.filter(|st| !st.underlying && user_ids.contains(&st.namespace.0))
				.collect();

			(
				user_namespaces,
				all_dictionaries,
				all_sumtypes,
				all_tables,
				all_ringbuffers,
				all_series,
				all_segment_trees,
			)
		};

		let mut resolver = NameResolver::empty();
		for ns in &user_namespaces {
			resolver.namespaces.insert(ns.id().0, ns.name().to_string());
		}
		for dictionary in &all_dictionaries {
			let qualified = qualify(&resolver, dictionary.namespace.0, &dictionary.name);
			resolver.dictionaries.insert(
				dictionary.id.to_u64(),
				ResolvedDictionary {
					qualified_name: qualified,
					value_type: dictionary.value_type.clone(),
				},
			);
		}
		for sumtype in &all_sumtypes {
			let qualified = qualify(&resolver, sumtype.namespace.0, &sumtype.name);
			let mut variants: Vec<ResolvedVariant> = sumtype
				.variants
				.iter()
				.map(|v| ResolvedVariant {
					tag: v.tag,
					name: v.name.clone(),
					fields: v.fields.iter().map(|f| f.name.clone()).collect(),
				})
				.collect();
			variants.sort_by_key(|v| v.tag);
			resolver.sumtypes.insert(
				sumtype.id.to_u64(),
				ResolvedSumType {
					qualified_name: qualified,
					variants,
				},
			);
		}

		let tables: Vec<Table> = all_tables
			.into_iter()
			.filter(|t| select_shape(options, &resolver, t.namespace.0, &t.name, ShapeKind::Table))
			.collect();
		let ringbuffers: Vec<RingBuffer> = all_ringbuffers
			.into_iter()
			.filter(|r| select_shape(options, &resolver, r.namespace.0, &r.name, ShapeKind::RingBuffer))
			.collect();
		let series: Vec<Series> = all_series
			.into_iter()
			.filter(|s| select_shape(options, &resolver, s.namespace.0, &s.name, ShapeKind::Series))
			.collect();
		let segment_trees: Vec<SegmentTree> = all_segment_trees
			.into_iter()
			.filter(|st| select_shape(options, &resolver, st.namespace.0, &st.name, ShapeKind::SegmentTree))
			.collect();

		let mut referenced_dicts: HashSet<u64> = HashSet::new();
		let mut referenced_sumtypes: HashSet<u64> = HashSet::new();
		for columns in tables
			.iter()
			.map(|t| &t.columns)
			.chain(ringbuffers.iter().map(|r| &r.columns))
			.chain(series.iter().map(|s| &s.columns))
			.chain(segment_trees.iter().map(|st| &st.columns))
		{
			collect_referenced(columns, &mut referenced_dicts, &mut referenced_sumtypes);
		}
		for s in &series {
			if let Some(tag) = &s.tag {
				referenced_sumtypes.insert(tag.to_u64());
			}
		}

		let dictionaries: Vec<Dictionary> = all_dictionaries
			.into_iter()
			.filter(|d| {
				referenced_dicts.contains(&d.id.to_u64())
					|| select_shape(
						options,
						&resolver,
						d.namespace.0,
						&d.name,
						ShapeKind::Dictionary,
					)
			})
			.collect();
		let sumtypes: Vec<SumType> = all_sumtypes
			.into_iter()
			.filter(|st| {
				let referenced = referenced_sumtypes.contains(&st.id.to_u64());
				let selected = matches!(st.kind, SumTypeKind::Enum)
					&& select_shape(options, &resolver, st.namespace.0, &st.name, ShapeKind::Enum);
				referenced || selected
			})
			.collect();

		let mut needed_namespaces: HashSet<u64> = HashSet::new();
		for t in &tables {
			needed_namespaces.insert(t.namespace.0);
		}
		for r in &ringbuffers {
			needed_namespaces.insert(r.namespace.0);
		}
		for s in &series {
			needed_namespaces.insert(s.namespace.0);
		}
		for st in &segment_trees {
			needed_namespaces.insert(st.namespace.0);
		}
		for d in &dictionaries {
			needed_namespaces.insert(d.namespace.0);
		}
		for st in &sumtypes {
			needed_namespaces.insert(st.namespace.0);
		}
		let namespaces: Vec<Namespace> = user_namespaces
			.into_iter()
			.filter(|ns| needed_namespaces.contains(&ns.id().0) && ns.id() != NamespaceId::DEFAULT)
			.collect();

		let include_data = options.includes_data();

		let mut table_exports = Vec::with_capacity(tables.len());
		for table in tables {
			let rows = if include_data {
				Some(self.read_shape_rows(&resolver, table.namespace.0, &table.name)?)
			} else {
				None
			};
			table_exports.push(TableExport {
				table,
				rows,
			});
		}

		let mut ringbuffer_exports = Vec::with_capacity(ringbuffers.len());
		for ringbuffer in ringbuffers {
			let rows = if include_data {
				Some(self.read_shape_rows(&resolver, ringbuffer.namespace.0, &ringbuffer.name)?)
			} else {
				None
			};
			ringbuffer_exports.push(RingBufferExport {
				ringbuffer,
				rows,
			});
		}

		let mut series_exports = Vec::with_capacity(series.len());
		for s in series {
			let rows = if include_data {
				Some(self.read_shape_rows(&resolver, s.namespace.0, &s.name)?)
			} else {
				None
			};
			series_exports.push(SeriesExport {
				series: s,
				rows,
			});
		}

		let mut segment_tree_exports = Vec::with_capacity(segment_trees.len());
		for segment_tree in segment_trees {
			let rows = if include_data {
				Some(self.read_shape_rows(&resolver, segment_tree.namespace.0, &segment_tree.name)?)
			} else {
				None
			};
			segment_tree_exports.push(SegmentTreeExport {
				segment_tree,
				rows,
			});
		}

		Ok(ExportModel {
			namespaces,
			sumtypes,
			dictionaries,
			tables: table_exports,
			ringbuffers: ringbuffer_exports,
			series: series_exports,
			segment_trees: segment_tree_exports,
			resolver,
		})
	}

	fn read_shape_rows(&self, resolver: &NameResolver, namespace_id: u64, name: &str) -> Result<ShapeRows> {
		let ns = resolver.namespaces.get(&namespace_id).ok_or_else(|| {
			Error(Box::new(internal!("namespace id {} not resolvable for export", namespace_id)))
		})?;
		let frames = self.query_as_root(&format!("from {}::{}", ns, name), Params::None)?;

		let mut columns: Vec<String> = Vec::new();
		let mut rows: Vec<Vec<Value>> = Vec::new();
		for frame in &frames {
			for row in frame.to_rows() {
				if columns.is_empty() {
					columns = row.iter().map(|(k, _)| k.clone()).collect();
				}
				rows.push(row.into_iter().map(|(_, v)| v).collect());
			}
		}
		Ok(ShapeRows {
			columns,
			rows,
		})
	}
}

fn is_user_namespace(namespace: &Namespace) -> bool {
	if namespace.is_remote() {
		return false;
	}
	let id = namespace.id().0;
	id == NamespaceId::DEFAULT.0 || id > NamespaceId::SYSTEM_METRICS_PROFILER_ACTOR.0
}

fn qualify(resolver: &NameResolver, namespace_id: u64, name: &str) -> String {
	match resolver.namespaces.get(&namespace_id) {
		Some(ns) => format!("{}::{}", ns, name),
		None => name.to_string(),
	}
}

fn select_shape(
	options: &ExportOptions,
	resolver: &NameResolver,
	namespace_id: u64,
	name: &str,
	kind: ShapeKind,
) -> bool {
	match &options.selection {
		ExportSelection::All => true,
		ExportSelection::Namespaces(names) => {
			resolver.namespaces.get(&namespace_id).map(|ns| names.iter().any(|n| n == ns)).unwrap_or(false)
		}
		ExportSelection::Kinds(kinds) => kinds.contains(&kind),
		ExportSelection::Shapes(shapes) => match resolver.namespaces.get(&namespace_id) {
			Some(ns) => shapes.iter().any(|s| &s.namespace == ns && s.name == name),
			None => false,
		},
	}
}

fn collect_referenced(columns: &[Column], dicts: &mut HashSet<u64>, sumtypes: &mut HashSet<u64>) {
	for column in columns {
		match column.constraint.constraint() {
			Some(Constraint::Dictionary(id, _)) => {
				dicts.insert(id.to_u64());
			}
			Some(Constraint::SumType(id)) => {
				sumtypes.insert(id.to_u64());
			}
			_ => {}
		}
		if let Some(id) = &column.dictionary_id {
			dicts.insert(id.to_u64());
		}
	}
}
