// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use indexmap::IndexMap;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::{OperatorState, decode},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	value::column::columns::Columns,
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::instrument;

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{
		Operator,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState},
		drops::SealedDrops,
		stateful::{raw::RawStatefulOperator, utils},
	},
	transaction::FlowTransaction,
};

const LAYOUT_KEY_PREFIX: u8 = 0x02;

const DROP_REASON: &str = "removes whose distinct entry was reclaimed";

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

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

pub struct DistinctPlan {
	parent_schema: Option<Columns>,
	pub(super) operator: OperatorId,
	pub(super) compiled_expressions: Vec<CompiledExpr>,
	pub(super) routines: Routines,
	pub(super) runtime_context: RuntimeContext,
	pub(super) ctx: Arc<FlowContext>,
	pub(super) dropped: SealedDrops,
	pub(super) _ttl: Option<Duration>,
}

pub struct DistinctOperator {
	pub(super) plan: Arc<DistinctPlan>,
	state: DistinctWorkingSet,
	hydrated: bool,
}

impl DistinctOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
		ttl: Option<Duration>,
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
			plan: Arc::new(DistinctPlan {
				parent_schema,
				operator,
				compiled_expressions,
				routines,
				runtime_context,
				ctx,
				dropped: SealedDrops::new(operator, DROP_REASON),
				_ttl: ttl,
			}),
			state: DistinctWorkingSet {
				state: DistinctState {
					entries: IndexMap::new(),
					layout: DistinctLayout::new(),
					dirty: HashMap::new(),
					layout_changed_at: None,
				},
				loaded: HashSet::new(),
				groups: HashMap::new(),
			},
			hydrated: false,
		}
	}

	fn hydrate_once<T: FlowTransaction>(&mut self, txn: &mut T) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.state.state.layout = self.plan.load_layout(txn)?;
		self.hydrated = true;
		Ok(())
	}

	fn flush_state<T: FlowTransaction>(&mut self, txn: &mut T) -> Result<()> {
		let plan = self.plan.clone();
		let working = &mut self.state;
		let dirty: Vec<(Hash128, DateTime)> = working.state.dirty.drain().collect();
		for (hash, at) in dirty {
			let key = DistinctPlan::entry_key(working.groups[&hash]);
			match working.state.entries.get(&hash) {
				Some(entry) => {
					let row = entry.encode_state(at).map_err(|e| {
						Error::from(FlowStateError::Encode {
							state: "DistinctEntry",
							cause: e.to_string(),
						})
					})?;
					utils::state_set(plan.operator, txn, &key, row)?;
				}
				None => utils::state_remove(plan.operator, txn, &key)?,
			}
		}
		if let Some(at) = working.state.layout_changed_at.take() {
			let layout_row = working.state.layout.encode_state(at).map_err(|e| {
				Error::from(FlowStateError::Encode {
					state: "DistinctLayout",
					cause: e.to_string(),
				})
			})?;
			utils::state_set(plan.operator, txn, &DistinctPlan::layout_storage_key(), layout_row)?;
		}
		Ok(())
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.plan.parent_schema.clone()
	}
}

impl DistinctPlan {
	pub(super) fn group_bytes(hash: Hash128) -> EncodedKey {
		EncodedKey::new(hash.0.to_be_bytes())
	}

	pub(super) fn entry_key(group: GroupId) -> GroupStateKey {
		OperatorStateKey::inner_encoded(group, Keyspace::DISTINCT_ENTRY, vec![])
	}

	pub(super) fn layout_storage_key() -> GroupStateKey {
		GroupStateKey::root(Keyspace::DISTINCT_LAYOUT, vec![LAYOUT_KEY_PREFIX])
	}

	#[instrument(name = "flow::operator::distinct::load_entry", level = "trace", skip_all)]
	fn load_entry<T: FlowTransaction>(&self, txn: &mut T, group: GroupId) -> Result<LoadedEntry> {
		match utils::state_get(self.operator, txn, &Self::entry_key(group))? {
			Some(row) => {
				if row.is_empty() {
					return Ok(LoadedEntry::Empty);
				}
				let entry: DistinctEntry = decode(&row).map_err(|e| {
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

	#[instrument(name = "flow::operator::distinct::load_layout", level = "trace", skip_all)]
	fn load_layout<T: FlowTransaction>(&self, txn: &mut T) -> Result<DistinctLayout> {
		match utils::state_get(self.operator, txn, &Self::layout_storage_key())? {
			Some(row) => {
				if row.is_empty() {
					return Ok(DistinctLayout::new());
				}
				decode(&row).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "DistinctLayout",
						cause: e.to_string(),
					})
				})
			}
			None => Ok(DistinctLayout::new()),
		}
	}

	#[instrument(name = "flow::operator::distinct::batch_hashes", level = "trace", skip_all, fields(diffs = diffs.len()))]
	fn batch_hashes(&self, diffs: &[Diff]) -> Result<Vec<Hash128>> {
		let mut touched: Vec<Hash128> = Vec::new();
		let mut seen: HashSet<Hash128> = HashSet::new();
		let mut fold = |hashes: Vec<Hash128>, touched: &mut Vec<Hash128>| {
			for hash in hashes {
				if seen.insert(hash) {
					touched.push(hash);
				}
			}
		};
		for diff in diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => fold(self.compute_hashes(post)?, &mut touched),
				Diff::Update {
					pre,
					post,
					..
				} => {
					fold(self.compute_hashes(pre)?, &mut touched);
					fold(self.compute_hashes(post)?, &mut touched);
				}
				Diff::Remove {
					pre,
					..
				} => fold(self.compute_hashes(pre)?, &mut touched),
			}
		}
		Ok(touched)
	}
}

impl<T: FlowTransaction> RawStatefulOperator<T> for DistinctOperator {}

impl<T: FlowTransaction> Operator<T> for DistinctOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn apply(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		self.hydrate_once(txn)?;

		let plan = self.plan.clone();
		let operator_id = plan.operator;
		let ordered = plan.batch_hashes(&change.diffs)?;
		let working = &mut self.state;

		let group_keys: Vec<EncodedKey> =
			ordered.iter().map(|hash| DistinctPlan::group_bytes(*hash)).collect();
		let interned = txn.intern_groups(operator_id, &group_keys)?;
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
				match plan.load_entry(txn, working.groups[&hash])? {
					LoadedEntry::Present(entry) => {
						working.state.entries.insert(hash, entry);
					}
					LoadedEntry::Empty => {
						working.state.dirty.insert(hash, DateTime::default());
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
						plan.process_insert(txn, &mut working.state, &working.groups, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result = plan.process_update(
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
						plan.process_remove(txn, &mut working.state, &working.groups, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		Ok(Change::from_flow(operator_id, change.version, result, change.changed_at))
	}

	fn flush(&mut self, txn: &mut T) -> Result<()> {
		self.flush_state(txn)
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
