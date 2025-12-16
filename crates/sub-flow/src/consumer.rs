// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::BTreeMap, path::PathBuf};

use indexmap::IndexMap;
use reifydb_catalog::{
	CatalogStore,
	resolve::{resolve_ringbuffer, resolve_table, resolve_view},
};
use reifydb_cdc::CdcConsume;
use reifydb_core::{
	CommitVersion, CowVec, Error, Result, Row,
	interface::{
		Cdc, CdcChange, ColumnDef, DictionaryDef, EncodableKey, Engine, KeyKind, MultiVersionQueryTransaction,
		SourceId, WithEventBus,
	},
	key::{DictionaryEntryIndexKey, Key},
	value::encoded::{EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout},
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardRowEvaluator};
use reifydb_flow_operator_sdk::FlowDiff;
use reifydb_rql::flow::{Flow, load_flow};
use reifydb_sub_api::SchedulerService;
use reifydb_type::{DictionaryEntryId, RowNumber, Value, internal};
use tracing::{instrument, trace};

use crate::{
	builder::OperatorFactory,
	engine::FlowEngine,
	operator::TransformOperatorRegistry,
	subsystem::intercept::Change,
	worker::{ParallelWorkerPool, SameThreadedWorker, WorkerPool},
};

/// Consumer that processes CDC events for Flow subsystem
pub struct FlowConsumer {
	engine: StandardEngine,
	flow_engine: FlowEngine,
	scheduler: Option<SchedulerService>,
}

impl FlowConsumer {
	pub fn new(
		engine: StandardEngine,
		operators: Vec<(String, OperatorFactory)>,
		operators_dir: Option<PathBuf>,
		scheduler: Option<SchedulerService>,
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
			scheduler,
		};

		if let Ok(mut txn) = engine.begin_command() {
			if let Ok(flows) = result.load_flows() {
				for flow in flows {
					result.flow_engine.register_without_backfill(&mut txn, flow).unwrap();
				}
				txn.commit().unwrap();
			}
		}

		result
	}

	/// Helper method to convert encoded bytes to Row format with dictionary decoding
	#[instrument(
		name = "flow::create_row",
		level = "trace",
		skip(txn, row_bytes),
		fields(
			source = ?source,
			row_number = row_number.0,
			row_bytes_len = row_bytes.len(),
		)
	)]
	fn create_row(
		txn: &mut StandardCommandTransaction,
		source: SourceId,
		row_number: RowNumber,
		row_bytes: Vec<u8>,
	) -> Result<Row> {
		// Get source metadata and columns from catalog
		let columns: Vec<ColumnDef> = match source {
			SourceId::Table(table_id) => {
				let resolved_table = resolve_table(txn, table_id)?;
				resolved_table.def().columns.clone()
			}
			SourceId::View(view_id) => {
				let resolved_view = resolve_view(txn, view_id)?;
				resolved_view.def().columns.clone()
			}
			SourceId::Flow(_flow_id) => {
				unimplemented!("Flow sources not supported in flows")
			}
			SourceId::TableVirtual(_) => {
				unimplemented!("Virtual table sources not supported in flows")
			}
			SourceId::RingBuffer(ringbuffer_id) => {
				let resolved_ringbuffer = resolve_ringbuffer(txn, ringbuffer_id)?;
				resolved_ringbuffer.def().columns.clone()
			}
			SourceId::Dictionary(_) => {
				unimplemented!("Dictionary sources not supported in flows")
			}
		};

		// Build storage types and dictionary info for each column
		let mut storage_types = Vec::with_capacity(columns.len());
		let mut value_types = Vec::with_capacity(columns.len());
		let mut dictionaries: Vec<Option<DictionaryDef>> = Vec::with_capacity(columns.len());
		let mut has_dictionary_columns = false;

		for col in &columns {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = CatalogStore::find_dictionary(txn, dict_id)? {
					storage_types.push(dict.id_type);
					value_types.push((col.name.clone(), dict.value_type));
					dictionaries.push(Some(dict));
					has_dictionary_columns = true;
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

		let raw_encoded = EncodedValues(CowVec::new(row_bytes));

		// If no dictionary columns, return directly with value layout
		if !has_dictionary_columns {
			let layout = EncodedValuesNamedLayout::new(value_types);
			return Ok(Row {
				number: row_number,
				encoded: raw_encoded,
				layout,
			});
		}

		// Decode dictionary columns
		let storage_layout = EncodedValuesLayout::new(&storage_types);
		let value_layout = EncodedValuesNamedLayout::new(value_types);

		let mut values: Vec<Value> = Vec::with_capacity(dictionaries.len());
		for (idx, dict_opt) in dictionaries.iter().enumerate() {
			let raw_value = storage_layout.get_value(&raw_encoded, idx);

			if let Some(dictionary) = dict_opt {
				// Decode dictionary ID to actual value
				if let Some(entry_id) = DictionaryEntryId::from_value(&raw_value) {
					let index_key =
						DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64)
							.encode();
					match txn.get(&index_key)? {
						Some(v) => {
							let (decoded_value, _): (Value, _) =
								bincode::serde::decode_from_slice(
									&v.values,
									bincode::config::standard(),
								)
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
	fn load_flows(&self) -> Result<Vec<Flow>> {
		let mut flows = Vec::new();
		let mut txn = self.engine.begin_query()?;

		// Get all flows from the catalog
		let flow_defs = reifydb_catalog::CatalogStore::list_flows_all(&mut txn)?;

		// Load each flow by reconstructing from nodes and edges
		for flow_def in flow_defs {
			match load_flow(&mut txn, flow_def.id) {
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

impl CdcConsume for FlowConsumer {
	#[instrument(
		name = "flow::consume",
		level = "trace",
		skip(self, txn, cdcs),
		fields(
			cdc_count = cdcs.len(),
		)
	)]
	fn consume(&self, txn: &mut StandardCommandTransaction, cdcs: Vec<Cdc>) -> Result<()> {
		if cdcs.is_empty() {
			return Ok(());
		}

		// Collect all changes grouped by version
		let mut changes_by_version: BTreeMap<CommitVersion, Vec<(SourceId, Change)>> = BTreeMap::new();
		let mut flows_changed_at_version: Option<CommitVersion> = None;

		for cdc in cdcs {
			let version = cdc.version;
			trace!("[CONSUMER] Processing CDC version={} with {} changes", version.0, cdc.changes.len());

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
							trace!(
								"[CONSUMER] Flow-related change (kind={:?}) at version={}, will reload flows",
								kind, version.0
							);
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
								_source_id: source_id,
								row_number: table_row.row,
								post: post.to_vec(),
							},
							CdcChange::Update {
								key: _,
								pre,
								post,
							} => Change::Update {
								_source_id: source_id,
								row_number: table_row.row,
								pre: pre.to_vec(),
								post: post.to_vec(),
							},
							CdcChange::Delete {
								key: _,
								pre,
							} => Change::Delete {
								_source_id: source_id,
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

		// Log CDC changes collected
		for (version, changes) in &changes_by_version {
			let rows: Vec<_> = changes
				.iter()
				.map(|(src, ch)| {
					let row = match ch {
						Change::Insert {
							row_number,
							..
						} => format!("I{}", row_number.0),
						Change::Update {
							row_number,
							..
						} => format!("U{}", row_number.0),
						Change::Delete {
							row_number,
							..
						} => format!("R{}", row_number.0),
					};
					format!("{}:{}", src.as_u64(), row)
				})
				.collect();
			trace!("[CONSUMER] CDC_IN version={} changes=[{}]", version.0, rows.join(","));
		}

		// Reload flows if needed (before processing any changes)
		// Only skip backfill for flows that already existed (they already have data)
		// New flows need backfill to get initial data from source tables
		if let Some(flow_creation_version) = flows_changed_at_version {
			let existing_flow_ids = self.flow_engine.flow_ids();
			trace!(
				"[Consumer] Reloading flows at version {:?}, existing_flow_ids={:?}",
				flow_creation_version, existing_flow_ids
			);
			self.flow_engine.clear();
			let flows = self.load_flows()?;
			trace!("[Consumer] Loaded {} flows from catalog", flows.len());
			for flow in flows {
				// For new flows: do backfill at this version
				// For existing flows: skip backfill (data already present)
				let is_existing = existing_flow_ids.contains(&flow.id);
				trace!(
					"[Consumer] Registering flow {:?}, is_existing={}, backfill_version={:?}",
					flow.id,
					is_existing,
					if is_existing {
						None
					} else {
						Some(flow_creation_version)
					}
				);
				if is_existing {
					self.flow_engine.register_without_backfill(txn, flow)?;
				} else {
					self.flow_engine.register_with_backfill(txn, flow, flow_creation_version)?;
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
						let row = Self::create_row(txn, source_id, row_number, post)?;
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
						let pre_row = Self::create_row(txn, source_id, row_number, pre)?;
						let post_row = Self::create_row(txn, source_id, row_number, post)?;
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
						let row = Self::create_row(txn, source_id, row_number, pre)?;
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
		let units = self.flow_engine.create_partition(diffs_by_version);
		if units.is_empty() {
			return Ok(());
		}

		// Process all flow units through the worker
		// Use parallel worker pool if scheduler is available, otherwise fall back to single-threaded
		let worker: Box<dyn WorkerPool> = if let Some(scheduler) = &self.scheduler {
			Box::new(ParallelWorkerPool::new(scheduler.clone()))
		} else {
			Box::new(SameThreadedWorker::new())
		};
		// let worker = Box::new(SameThreadedWorker::new());
		worker.process(txn, units, &self.flow_engine)
	}
}
