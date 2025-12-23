// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogSourceQueryOperations, CatalogStore};
use reifydb_core::{
	CommitVersion, Error, Row,
	interface::{
		ColumnDef, DictionaryDef, EncodableKey, FlowNodeId, MultiVersionQueryTransaction, RowKey, RowKeyRange,
		SourceDef, SourceId,
	},
	value::encoded::{EncodedValuesLayout, EncodedValuesNamedLayout},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_rql::flow::{Flow, FlowNodeType};
use reifydb_type::{DictionaryEntryId, Value, internal};
use tracing::{info, instrument};

use crate::{engine::FlowEngine, transaction::FlowTransaction};

impl FlowEngine {
	#[instrument(name = "flow::backfill::load", level = "info", skip(self, txn), fields(flow_id = ?flow.id, flow_creation_version = flow_creation_version.0))]
	pub(crate) async fn load_initial_data(
		&self,
		txn: &mut StandardCommandTransaction,
		flow: &Flow,
		flow_creation_version: CommitVersion,
	) -> crate::Result<()> {
		// Collect all source nodes in topological order
		let mut source_nodes = Vec::new();
		for node_id in flow.topological_order()? {
			if let Some(node) = flow.get_node(&node_id) {
				match &node.ty {
					FlowNodeType::SourceTable {
						..
					}
					| FlowNodeType::SourceView {
						..
					} => {
						source_nodes.push(node.clone());
					}
					_ => {}
				}
			}
		}

		let backfill_version = CommitVersion(flow_creation_version.0.saturating_sub(1));
		let mut flow_txn = FlowTransaction::new(txn, backfill_version).await;
		let mut source_changes: Vec<(FlowNodeId, FlowChange)> = Vec::new();

		for source_node in &source_nodes {
			let source_id = match &source_node.ty {
				FlowNodeType::SourceTable {
					table,
				} => SourceId::table(*table),
				FlowNodeType::SourceView {
					view,
				} => SourceId::view(*view),
				_ => continue,
			};

			let source_def = txn.get_source(source_id).await?;
			let rows = self.scan_all_rows(txn, &mut flow_txn, &source_def).await?;
			if rows.is_empty() {
				continue;
			}

			let (namespace, name) = match &source_def {
				SourceDef::Table(t) => (&t.namespace, &t.name),
				SourceDef::View(v) => (&v.namespace, &v.name),
				_ => unreachable!("Only Table and View sources are supported for backfill"),
			};

			info!("[INITIAL_LOAD] Processing {} rows from source {}.{}", rows.len(), namespace, name);

			let diffs: Vec<FlowDiff> = rows
				.into_iter()
				.map(|row| FlowDiff::Insert {
					post: row,
				})
				.collect();

			let change = FlowChange {
				origin: FlowChangeOrigin::Internal(source_node.id),
				version: flow_creation_version,
				diffs,
			};

			// Apply through source operator to get transformed change
			let operators = self.inner.operators.read().await;
			let source_operator = operators
				.get(&source_node.id)
				.ok_or_else(|| Error(internal!("Source operator not found")))?
				.clone();
			drop(operators);

			let result_change = source_operator.apply(&mut flow_txn, change, &self.inner.evaluator).await?;
			if !result_change.diffs.is_empty() {
				source_changes.push((source_node.id, result_change));
			}
		}

		// Phase 2: Propagate all source changes through downstream operators
		// Now all JOIN sides have their data in state
		for (source_node_id, change) in source_changes {
			self.propagate_initial_change(&mut flow_txn, flow, source_node_id, change).await?;
		}

		flow_txn.commit(txn).await?;

		Ok(())
	}

	// FIXME this can be streamed without loading everything into memory first
	#[instrument(name = "flow::backfill::scan", level = "debug", skip(self, txn, flow_txn), fields(source_id = ?source.id()))]
	async fn scan_all_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		flow_txn: &mut FlowTransaction,
		source: &SourceDef,
	) -> crate::Result<Vec<Row>> {
		let mut rows = Vec::new();

		// Get column definitions from the source
		let columns: &[ColumnDef] = match source {
			SourceDef::Table(t) => &t.columns,
			SourceDef::View(v) => &v.columns,
			_ => unreachable!("Only Table and View sources are supported for backfill"),
		};

		// Build storage types and dictionary info for each column
		let mut storage_types = Vec::with_capacity(columns.len());
		let mut value_types = Vec::with_capacity(columns.len());
		let mut dictionaries: Vec<Option<DictionaryDef>> = Vec::with_capacity(columns.len());

		for col in columns {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = CatalogStore::find_dictionary(txn, dict_id).await? {
					storage_types.push(dict.id_type);
					value_types.push((col.name.clone(), dict.value_type));
					dictionaries.push(Some(dict));
				} else {
					// Dictionary not found, fall back to constraint type
					storage_types.push(col.constraint.get_type());
					value_types.push((col.name.clone(), col.constraint.get_type()));
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				value_types.push((col.name.clone(), col.constraint.get_type()));
				dictionaries.push(None);
			}
		}

		// Layout for reading raw storage (with dictionary ID types)
		let storage_layout = EncodedValuesLayout::new(&storage_types);
		// Layout for the decoded row (with actual value types)
		let value_layout = EncodedValuesNamedLayout::new(value_types);

		let range = RowKeyRange::scan_range(source.id(), None);

		const BATCH_SIZE: u64 = 10000;
		let multi_rows: Vec<_> =
			flow_txn.range_batched(range.into(), BATCH_SIZE).await?.items.into_iter().collect();

		for multi in multi_rows {
			if let Some(key) = RowKey::decode(&multi.key) {
				// Decode dictionary columns and re-encode with value types
				let decoded_encoded = self
					.decode_dictionary_row(
						txn,
						&multi.values,
						&storage_layout,
						&value_layout,
						&dictionaries,
					)
					.await?;

				rows.push(Row {
					number: key.row,
					encoded: decoded_encoded,
					layout: value_layout.clone(),
				});
			}
		}

		// Sort by row number to ensure deterministic ordering for "first occurrence" semantics
		rows.sort_by_key(|row| row.number);

		Ok(rows)
	}

	/// Decode dictionary columns in a row by looking up dictionary values
	async fn decode_dictionary_row(
		&self,
		txn: &mut StandardCommandTransaction,
		raw_encoded: &reifydb_core::value::encoded::EncodedValues,
		storage_layout: &EncodedValuesLayout,
		value_layout: &EncodedValuesNamedLayout,
		dictionaries: &[Option<DictionaryDef>],
	) -> crate::Result<reifydb_core::value::encoded::EncodedValues> {
		// Extract values using storage layout
		let mut values: Vec<Value> = Vec::with_capacity(dictionaries.len());

		for (idx, dict_opt) in dictionaries.iter().enumerate() {
			let raw_value = storage_layout.get_value(raw_encoded, idx);

			if let Some(dictionary) = dict_opt {
				// This is a dictionary column - decode the ID to the actual value
				if let Some(entry_id) = DictionaryEntryId::from_value(&raw_value) {
					// Look up the value in the dictionary
					let index_key = reifydb_core::key::DictionaryEntryIndexKey::new(
						dictionary.id,
						entry_id.to_u128() as u64,
					)
					.encode();
					match txn.get(&index_key).await? {
						Some(v) => {
							let decoded_value: Value = postcard::from_bytes(&v.values)
								.map_err(|e| {
									Error(internal!(
										"Failed to deserialize dictionary value: {}",
										e
									))
								})?;
							values.push(decoded_value);
						}
						None => {
							// ID not found in dictionary, use undefined
							values.push(Value::Undefined);
						}
					}
				} else {
					// Not a valid dictionary ID (e.g., undefined), keep as-is
					values.push(raw_value);
				}
			} else {
				// Not a dictionary column, keep the raw value
				values.push(raw_value);
			}
		}

		// Re-encode with value layout
		let mut encoded = value_layout.allocate();
		value_layout.set_values(&mut encoded, &values);

		Ok(encoded)
	}

	#[instrument(name = "flow::backfill::propagate", level = "debug", skip(self, flow_txn, flow), fields(from_node = ?from_node_id, diff_count = change.diffs.len()))]
	fn propagate_initial_change<'a>(
		&'a self,
		flow_txn: &'a mut FlowTransaction,
		flow: &'a Flow,
		from_node_id: FlowNodeId,
		change: FlowChange,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<()>> + Send + 'a>> {
		Box::pin(async move {
			let downstream_nodes = flow
				.graph
				.nodes()
				.filter(|(_, node)| node.inputs.contains(&from_node_id))
				.map(|(id, _)| *id)
				.collect::<Vec<_>>();

			for downstream_node_id in downstream_nodes {
				let operator = {
					let operators = self.inner.operators.read().await;
					operators.get(&downstream_node_id).cloned()
				};

				if let Some(operator) = operator {
					let result =
						operator.apply(flow_txn, change.clone(), &self.inner.evaluator).await?;
					if !result.diffs.is_empty() {
						self.propagate_initial_change(
							flow_txn,
							flow,
							downstream_node_id,
							result,
						)
						.await?;
					}
				}
			}

			Ok(())
		})
	}
}
