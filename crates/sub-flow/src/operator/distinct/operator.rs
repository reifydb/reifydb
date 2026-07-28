// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::{HashMap, HashSet},
	mem::size_of,
	sync::Arc,
};

use indexmap::IndexMap;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
	metrics::heap::HeapSize,
	state::horizon::Position,
	value::column::columns::Columns,
};
use reifydb_engine::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_flow::{
	operator::Operator,
	transaction::{FlowTransaction, slot::PersistFn},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, byte_size::ByteSize, error::Error, util::hash::Hash128};

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{
		OperatorCell,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState},
		stateful::{raw::RawStatefulOperator, single::SingleStateful, utils},
	},
};

const LAYOUT_KEY_PREFIX: u8 = 0x02;

const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

pub(super) struct DistinctWorkingSet {
	pub(super) state: DistinctState,
	pub(super) loaded: HashSet<Hash128>,
	pub(super) groups: HashMap<Hash128, GroupId>,
}

enum LoadedEntry {
	Absent,
	Empty,
	Present(DistinctEntry),
}

fn working_set_usage(value: &dyn Any) -> ByteSize {
	let working = value.downcast_ref::<DistinctWorkingSet>().expect("DistinctWorkingSet slot type");
	let groups = working.groups.capacity() * (size_of::<Hash128>() + size_of::<GroupId>());
	ByteSize::from_bytes(
		(size_of::<DistinctWorkingSet>() + working.state.heap_size() + working.loaded.heap_size() + groups)
			as u64,
	)
}

pub struct DistinctOperator {
	parent: OperatorCell,
	pub(super) node: FlowNodeId,
	pub(super) compiled_expressions: Vec<CompiledExpr>,
	pub(super) shape: RowShape,
	pub(super) routines: Routines,
	pub(super) runtime_context: RuntimeContext,
	pub(super) ctx: Arc<FlowContext>,
}

impl DistinctOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			compiled_expressions,
			shape: RowShape::operator_state(),
			routines,
			runtime_context,
			ctx,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	pub(super) fn group_bytes(hash: Hash128) -> EncodedKey {
		EncodedKey::new(hash.0.to_be_bytes())
	}

	pub(super) fn entry_key(group: GroupId) -> StateKey {
		OperatorStateKey::inner_encoded(group, Keyspace::DISTINCT_ENTRY, vec![])
	}

	pub(super) fn layout_storage_key() -> StateKey {
		StateKey::node_scoped(Keyspace::DISTINCT_LAYOUT, vec![LAYOUT_KEY_PREFIX])
	}

	pub(super) fn state_bytes(row: EncodedRow, state: &'static str) -> Result<StateBytes> {
		StateBytes::from_row(row).map_err(|e| {
			Error::from(FlowStateError::Decode {
				state,
				cause: e.to_string(),
			})
		})
	}

	fn load_entry(&self, txn: &mut FlowTransaction, group: GroupId) -> Result<LoadedEntry> {
		match utils::state_get(self.node, txn, &Self::entry_key(group))? {
			Some(row) => {
				let bytes = Self::state_bytes(row, "DistinctEntry")?;
				if bytes.body().is_empty() {
					return Ok(LoadedEntry::Empty);
				}
				let entry: DistinctEntry = decode_state(&bytes).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "DistinctEntry",
						cause: e.to_string(),
					})
				})?;
				Ok(LoadedEntry::Present(entry))
			}
			None => Ok(LoadedEntry::Absent),
		}
	}

	fn load_layout(&self, txn: &mut FlowTransaction) -> Result<DistinctLayout> {
		match utils::state_get(self.node, txn, &Self::layout_storage_key())? {
			Some(row) => {
				let bytes = Self::state_bytes(row, "DistinctLayout")?;
				if bytes.body().is_empty() {
					return Ok(DistinctLayout::new());
				}
				decode_state(&bytes).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "DistinctLayout",
						cause: e.to_string(),
					})
				})
			}
			None => Ok(DistinctLayout::new()),
		}
	}

	fn batch_hashes(&self, diffs: &[Diff]) -> Result<HashSet<Hash128>> {
		let mut touched: HashSet<Hash128> = HashSet::new();
		for diff in diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => touched.extend(self.compute_hashes(post)?),
				Diff::Update {
					pre,
					post,
					..
				} => {
					touched.extend(self.compute_hashes(pre)?);
					touched.extend(self.compute_hashes(post)?);
				}
				Diff::Remove {
					pre,
					..
				} => touched.extend(self.compute_hashes(pre)?),
			}
		}
		Ok(touched)
	}
}

impl RawStatefulOperator for DistinctOperator {}

impl SingleStateful for DistinctOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let touched = self.batch_hashes(&change.diffs)?;

		let (mut working, persist) = txn.take_operator_state::<DistinctWorkingSet, _>(node_id, |txn| {
			let layout = self.load_layout(txn)?;
			let working = DistinctWorkingSet {
				state: DistinctState {
					entries: IndexMap::new(),
					layout,
					dirty: HashSet::new(),
					layout_dirty: false,
				},
				loaded: HashSet::new(),
				groups: HashMap::new(),
			};
			let persist: PersistFn = Box::new(move |txn, value| {
				let working =
					*value.downcast::<DistinctWorkingSet>().expect("DistinctWorkingSet slot type");
				let now = txn.clock().now();
				for hash in &working.state.dirty {
					let key = Self::entry_key(working.groups[hash]);
					match working.state.entries.get(hash) {
						Some(entry) => {
							let bytes = entry.encode_state(now).map_err(|e| {
								Error::from(FlowStateError::Encode {
									state: "DistinctEntry",
									cause: e.to_string(),
								})
							})?;
							utils::state_set(node_id, txn, &key, bytes.into_row())?;
						}
						None => utils::state_remove(node_id, txn, &key)?,
					}
				}
				if working.state.layout_dirty {
					let layout_bytes =
						working.state.layout.encode_state(now).map_err(|e| {
							Error::from(FlowStateError::Encode {
								state: "DistinctLayout",
								cause: e.to_string(),
							})
						})?;
					utils::state_set(
						node_id,
						txn,
						&Self::layout_storage_key(),
						layout_bytes.into_row(),
					)?;
				}
				Ok(())
			});
			Ok((working, persist))
		})?;

		let ordered: Vec<Hash128> = touched.into_iter().collect();
		let group_keys: Vec<EncodedKey> = ordered.iter().map(|hash| Self::group_bytes(*hash)).collect();
		let interned = txn.intern_groups(node_id, &group_keys, Position::Version(change.version.0))?;
		let mut fresh: HashMap<Hash128, bool> = HashMap::with_capacity(ordered.len());
		for (hash, (group, is_new)) in ordered.iter().zip(interned) {
			working.groups.insert(*hash, group);
			fresh.insert(*hash, is_new);
		}

		for &hash in &ordered {
			if working.loaded.insert(hash) {
				if fresh[&hash] {
					continue;
				}
				match self.load_entry(txn, working.groups[&hash])? {
					LoadedEntry::Present(entry) => {
						working.state.entries.insert(hash, entry);
					}
					LoadedEntry::Empty => {
						working.state.dirty.insert(hash);
					}
					LoadedEntry::Absent => {}
				}
			}
		}

		let mut result = Vec::new();
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let insert_result =
						self.process_insert(txn, &mut working.state, &working.groups, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result = self.process_update(
						txn,
						&mut working.state,
						&working.groups,
						&pre,
						&post,
					)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
					..
				} => {
					let remove_result =
						self.process_remove(txn, &mut working.state, &working.groups, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		txn.put_operator_state(node_id, working, persist, working_set_usage);

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}
}
