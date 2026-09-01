// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use indexmap::IndexMap;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
	value::column::columns::Columns,
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, error::Error, util::hash::Hash128, value::datetime::DateTime};
use tracing::instrument;

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{
		HostOperator,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState},
		drops::SealedDrops,
		host::HostContext,
		state::store,
	},
};

const DROP_REASON: &str = "removes whose distinct entry was reclaimed";

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

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
}

pub struct DistinctOperator {
	pub(super) plan: DistinctPlan,
}

impl DistinctOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
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
			plan: DistinctPlan {
				parent_schema,
				operator,
				compiled_expressions,
				routines,
				runtime_context,
				ctx,
				dropped: SealedDrops::new(operator, DROP_REASON),
			},
		}
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
		OperatorStateKey::inner_encoded(group, KeyspaceId::DISTINCT_ENTRY, vec![])
	}

	pub(super) fn layout_storage_key() -> GroupStateKey {
		GroupStateKey::root(KeyspaceId::DISTINCT_LAYOUT, vec![])
	}

	#[instrument(name = "flow::operator::distinct::load_entry", level = "trace", skip_all)]
	fn load_entry(&self, host: &mut dyn HostContext, group: GroupId) -> Result<LoadedEntry> {
		match store::state_get(host, &Self::entry_key(group))? {
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
	fn load_layout(&self, host: &mut dyn HostContext) -> Result<DistinctLayout> {
		match store::state_get(host, &Self::layout_storage_key())? {
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

	fn persist(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
	) -> Result<()> {
		let dirty: Vec<(Hash128, DateTime)> = state.dirty.drain().collect();
		for (hash, _) in dirty {
			let key = Self::entry_key(groups[&hash]);
			match state.entries.get(&hash) {
				Some(entry) => {
					let row = entry.encode_state().map_err(|e| {
						Error::from(FlowStateError::Encode {
							state: "DistinctEntry",
							cause: e.to_string(),
						})
					})?;
					store::state_set(host, &key, row)?;
				}
				None => store::state_remove(host, &key)?,
			}
		}
		if state.layout_changed_at.take().is_some() {
			let layout_row = state.layout.encode_state().map_err(|e| {
				Error::from(FlowStateError::Encode {
					state: "DistinctLayout",
					cause: e.to_string(),
				})
			})?;
			store::state_set(host, &Self::layout_storage_key(), layout_row)?;
		}
		Ok(())
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

impl HostOperator for DistinctOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let plan = &self.plan;
		let operator_id = plan.operator;
		let ordered = plan.batch_hashes(&change.diffs)?;

		let mut state = DistinctState {
			entries: IndexMap::new(),
			layout: plan.load_layout(host)?,
			dirty: HashMap::new(),
			layout_changed_at: None,
		};

		let mut groups: HashMap<Hash128, GroupId> = HashMap::with_capacity(ordered.len());
		for hash in ordered.iter() {
			let group = GroupId::of(&DistinctPlan::group_bytes(*hash));
			groups.insert(*hash, group);
			match plan.load_entry(host, group)? {
				LoadedEntry::Present(entry) => {
					state.entries.insert(*hash, entry);
				}
				LoadedEntry::Empty => {
					state.dirty.insert(*hash, DateTime::default());
				}
				LoadedEntry::Absent => {}
			}
		}

		let mut result = Vec::new();
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let insert_result = plan.process_insert(host, &mut state, &groups, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result =
						plan.process_update(host, &mut state, &groups, &pre, &post)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
					..
				} => {
					let remove_result = plan.process_remove(host, &mut state, &groups, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		plan.persist(host, &mut state, &groups)?;

		Ok(Change::from_flow(operator_id, change.version, result, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
