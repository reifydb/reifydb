// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::BTreeMap, path::PathBuf};

use indexmap::IndexMap;
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	CommitVersion, CowVec, Error, Result, Row,
	interface::{Cdc, CdcChange, EncodableKey, Engine, KeyKind, QueryTransaction, SourceId, WithEventBus},
	key::{DictionaryEntryIndexKey, Key},
	value::encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardRowEvaluator};
use reifydb_flow_operator_sdk::FlowDiff;
use reifydb_rql::flow::{Flow, load_flow};
use reifydb_type::{DictionaryEntryId, RowNumber, Value, internal};
use tracing::instrument;

use crate::{
	builder::OperatorFactory,
	catalog::FlowCatalog,
	engine::FlowEngine,
	operator::TransformOperatorRegistry,
	subsystem::intercept::Change,
	worker::{SameThreadedWorker, WorkerPool},
};

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer {
	engine: StandardEngine,
	flow_engine: FlowEngine,
	catalog_cache: FlowCatalog,
}

impl FlowConsumer {
	pub async fn new(
		engine: StandardEngine,
		operators: Vec<(String, OperatorFactory)>,
		operators_dir: Option<PathBuf>,
	) -> Self {
		let mut registry = TransformOperatorRegistry::new();

		for (name, factory) in operators.iter() {
			let factory = factory.clone();
			let name = name.clone();
			registry.register(name, move |node, exprs| factory(node, exprs));
		}

		let flow_engine = FlowEngine::new(
			StandardRowEvaluator::default(),
			engine.executor(),
			registry,
			engine.event_bus().clone(),
			operators_dir,
		);

		let result = Self {
			engine: engine.clone(),
			flow_engine,
			catalog_cache: FlowCatalog::new(),
		};

		if let Ok(mut txn) = engine.begin_command().await {
			if let Ok(flows) = result.load_flows().await {
				for flow in flows {
					result.flow_engine.register_without_backfill(&mut txn, flow).await.unwrap();
				}
				txn.commit().await.unwrap();
			}
		}

		result
	}

	/// Helper method to convert encoded bytes to Row format with dictionary decoding.
	///
	/// Uses `CatalogCache` to avoid repeated catalog lookups for the same source.
	/// The cache is populated on first access and invalidated when schema changes
	/// are observed via CDC.
	#[instrument(
		name = "flow::create_row",
		level = "trace",
		skip(self, txn, row_bytes),
		fields(
			source = ?source,
			row_number = row_number.0,
			row_bytes_len = row_bytes.len(),
		)
	)]
	async fn create_row(
		&self,
		txn: &mut StandardCommandTransaction,
		source: SourceId,
		row_number: RowNumber,
		row_bytes: Vec<u8>,
	) -> Result<Row> {
		// Get cached source metadata (loads from catalog on cache miss)
		let metadata = self.catalog_cache.get_or_load(txn, source).await?;

		let raw_encoded = EncodedValues(CowVec::new(row_bytes));

		// If no dictionary columns, return directly with value layout
		if !metadata.has_dictionary_columns {
			let layout = EncodedValuesNamedLayout::new(metadata.value_types.clone());
			return Ok(Row {
				number: row_number,
				encoded: raw_encoded,
				layout,
			});
		}

		// Decode dictionary columns
		let storage_layout = EncodedValuesLayout::new(&metadata.storage_types);
		let value_layout = EncodedValuesNamedLayout::new(metadata.value_types.clone());

		let mut values: Vec<Value> = Vec::with_capacity(metadata.dictionaries.len());
		for (idx, dict_opt) in metadata.dictionaries.iter().enumerate() {
			let raw_value = storage_layout.get_value(&raw_encoded, idx);

			if let Some(dictionary) = dict_opt {
				// Decode dictionary ID to actual value
				if let Some(entry_id) = DictionaryEntryId::from_value(&raw_value) {
					let index_key =
						DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64)
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
							values.push(Value::Undefined);
						}
					}
				} else {
					values.push(raw_value);
				}
			} else {
				values.push(raw_value);
			}
		}

		// Re-encode with value layout
		let mut encoded = value_layout.allocate();
		value_layout.set_values(&mut encoded, &values);

		Ok(Row {
			number: row_number,
			encoded,
			layout: value_layout,
		})
	}

	/// Load flows from the catalog
	async fn load_flows(&self) -> Result<Vec<Flow>> {
		let mut flows = Vec::new();
		let mut txn = self.engine.begin_query().await?;

		// Get all flows from the catalog
		let flow_defs = reifydb_catalog::CatalogStore::list_flows_all(&mut txn).await?;

		// Load each flow by reconstructing from nodes and edges
		for flow_def in flow_defs {
			match load_flow(&mut txn, flow_def.id).await {
				Ok(flow) => flows.push(flow),
				Err(e) => {
					// Log error but continue loading other flows
					eprintln!("Failed to load flow {}: {}", flow_def.name, e);
				}
			}
		}

		Ok(flows)
	}
}

#[async_trait::async_trait]
impl CdcConsume for FlowConsumer {
	#[instrument(
		name = "flow::consume",
		level = "trace",
		skip(self, txn, cdcs),
		fields(
			cdc_count = cdcs.len(),
		)
	)]
	async fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		self.consume_async(txn, cdcs).await
	}
}

impl FlowConsumer {
	async fn consume_async(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		if cdcs.is_empty() {
			return Ok(());
		}

		// Invalidate cache for any schema changes before processing
		for cdc in &cdcs {
			for change in &cdc.changes {
				self.catalog_cache.invalidate_from_cdc(change.key()).await;
			}
		}

		// Collect all changes grouped by version
		let mut changes_by_version: BTreeMap<CommitVersion, Vec<(SourceId, Change)>> = BTreeMap::new();
		let mut flows_changed_at_version: Option<CommitVersion> = None;

		for cdc in cdcs {
			let version = cdc.version;

			for sequenced_change in cdc.changes {
				// Check key kind first to detect flow-related changes
				if let Some(kind) = Key::kind(sequenced_change.key()) {
					// Detect any flow definition changes - trigger reload
					if matches!(
						kind,
						KeyKind::Flow
							| KeyKind::FlowNode | KeyKind::FlowNodeByFlow | KeyKind::FlowEdge
							| KeyKind::FlowEdgeByFlow | KeyKind::NamespaceFlow
					) {
						if flows_changed_at_version.is_none() {
							flows_changed_at_version = Some(version);
						}
						continue;
					}
				}

				// Then try to decode as Key::Row for data changes
				if let Some(decoded_key) = Key::decode(sequenced_change.key()) {
					if let Key::Row(table_row) = decoded_key {
						let source_id = table_row.source;

						// CDC now returns resolved values directly
						let change = match &sequenced_change.change {
							CdcChange::Insert {
								key: _,
								post,
							} => Change::Insert {
								row_number: table_row.row,
								post: post.to_vec(),
							},
							CdcChange::Update {
								key: _,
								pre,
								post,
							} => Change::Update {
								row_number: table_row.row,
								pre: pre.to_vec(),
								post: post.to_vec(),
							},
							CdcChange::Delete {
								key: _,
								pre,
							} => Change::Delete {
								row_number: table_row.row,
								pre: pre.as_ref()
									.map(|v| v.to_vec())
									.unwrap_or_default(),
							},
						};
						changes_by_version
							.entry(version)
							.or_insert_with(Vec::new)
							.push((source_id, change));
					}
				}
			}
		}

		// Reload flows if needed (before processing any changes)
		// Only skip backfill for flows that already existed (they already have data)
		// New flows need backfill to get initial data from source tables
		if let Some(flow_creation_version) = flows_changed_at_version {
			let existing_flow_ids = self.flow_engine.flow_ids().await;
			self.flow_engine.clear().await;
			let flows = self.load_flows().await?;
			for flow in flows {
				// For new flows: do backfill at this version
				// For existing flows: skip backfill (data already present)
				let is_existing = existing_flow_ids.contains(&flow.id);
				if is_existing {
					self.flow_engine.register_without_backfill(txn, flow).await?;
				} else {
					self.flow_engine
						.register_with_backfill(txn, flow, flow_creation_version)
						.await?;
				};
			}
		}

		// If no changes to process, we're done
		if changes_by_version.is_empty() {
			return Ok(());
		}

		let mut diffs_by_version: BTreeMap<CommitVersion, Vec<(SourceId, Vec<FlowDiff>)>> = BTreeMap::new();

		for (version, changes) in changes_by_version {
			// Group changes by source for this version
			// Using IndexMap to preserve CDC insertion order within the version
			let mut changes_by_source: IndexMap<SourceId, Vec<FlowDiff>> = IndexMap::new();

			for (source_id, change) in changes {
				let diff = match change {
					Change::Insert {
						row_number,
						post,
						..
					} => {
						let row = self.create_row(txn, source_id, row_number, post).await?;
						FlowDiff::Insert {
							post: row,
						}
					}
					Change::Update {
						row_number,
						pre,
						post,
						..
					} => {
						let pre_row = self.create_row(txn, source_id, row_number, pre).await?;
						let post_row =
							self.create_row(txn, source_id, row_number, post).await?;
						FlowDiff::Update {
							pre: pre_row,
							post: post_row,
						}
					}
					Change::Delete {
						row_number,
						pre,
						..
					} => {
						let row = self.create_row(txn, source_id, row_number, pre).await?;
						FlowDiff::Remove {
							pre: row,
						}
					}
				};
				changes_by_source.entry(source_id).or_insert_with(Vec::new).push(diff);
			}

			// Convert to Vec format expected by create_partition
			let source_diffs: Vec<(SourceId, Vec<FlowDiff>)> = changes_by_source.into_iter().collect();
			diffs_by_version.insert(version, source_diffs);
		}

		// Partition all changes across all versions into units of work
		let units = self.flow_engine.create_partition(diffs_by_version).await;
		if units.is_empty() {
			return Ok(());
		}

		let worker = SameThreadedWorker::new();
		worker.process(txn, units, &self.flow_engine).await
	}
}
