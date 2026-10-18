// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem::take,
	sync::Arc,
};

use arrow_schema::SchemaRef;
use indexmap::{IndexMap, IndexSet};
use reifydb_codec::row::{
	operator::state::{OperatorState, decode},
	pod::EncodedPodRow,
};
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
};
use reifydb_evaluate::lower::{LoweredExpr, compile_check};
use reifydb_flow::{context::FlowContext, operator::source::decode_change};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, error::Error, util::hash::Hash128};
use tracing::instrument;

use crate::{
	error::FlowStateError,
	operator::{
		HostOperator,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState},
		drops::SealedDrops,
		host::HostContext,
	},
};

const DROP_REASON: &str = "removes whose distinct entry was reclaimed";

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

type DiffHashes = (Vec<Hash128>, Vec<Hash128>);

enum LoadedEntry {
	Absent,
	Empty,
	Present(DistinctEntry),
}

pub struct DistinctPlan {
	parent_schema: Option<SchemaRef>,
	pub(super) operator: OperatorId,
	pub(super) lowered_expressions: Vec<LoweredExpr>,
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
		parent_schema: Option<SchemaRef>,
		operator: OperatorId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Result<Self> {
		for expression in &expressions {
			compile_check(expression)?;
		}
		let lowered_expressions: Vec<LoweredExpr> =
			expressions.iter().map(|e| LoweredExpr::new(e.clone(), "distinct")).collect();

		Ok(Self {
			plan: DistinctPlan {
				parent_schema,
				operator,
				lowered_expressions,
				routines,
				runtime_context,
				ctx,
				dropped: SealedDrops::new(operator, DROP_REASON),
			},
		})
	}

	pub(crate) fn output_schema(&self) -> Option<SchemaRef> {
		self.plan.parent_schema.clone()
	}
}

impl DistinctPlan {
	pub(super) fn entry_key(group: GroupId) -> GroupStateKey {
		OperatorStateKey::inner_encoded(group, KeyspaceId::DISTINCT_ENTRY, vec![])
	}

	pub(super) fn layout_storage_key() -> GroupStateKey {
		GroupStateKey::root(KeyspaceId::DISTINCT_LAYOUT, vec![])
	}

	fn decode_entry(row: Option<EncodedPodRow>) -> Result<LoadedEntry> {
		match row {
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

	fn decode_layout(row: Option<EncodedPodRow>) -> Result<DistinctLayout> {
		match row {
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
		let dirty: Vec<Hash128> = state.dirty.drain().collect();
		let mut sets = Vec::new();
		let mut removes = Vec::new();
		for hash in dirty {
			let key = Self::entry_key(groups[&hash]);
			match state.entries.get(&hash) {
				Some(entry) => {
					let row = entry.encode_state().map_err(|e| {
						Error::from(FlowStateError::Encode {
							state: "DistinctEntry",
							cause: e.to_string(),
						})
					})?;
					sets.push((key, row));
				}
				None => removes.push(key),
			}
		}
		if take(&mut state.layout_changed) {
			let layout_row = state.layout.encode_state().map_err(|e| {
				Error::from(FlowStateError::Encode {
					state: "DistinctLayout",
					cause: e.to_string(),
				})
			})?;
			sets.push((Self::layout_storage_key(), layout_row));
		}
		host.state_set_many(sets)?;
		host.state_remove_many(&removes)?;
		let withdrawn: Vec<GroupId> = state.withdrawn.drain(..).collect();
		host.remove_row_numbers_for_groups(&withdrawn)
	}

	#[instrument(name = "flow::operator::distinct::batch_hashes", level = "trace", skip_all, fields(diffs = diffs.len()))]
	fn batch_hashes(&self, diffs: &[Diff]) -> Result<(Vec<Hash128>, Vec<DiffHashes>)> {
		let mut touched: Vec<Hash128> = Vec::new();
		let mut seen: HashSet<Hash128> = HashSet::new();
		let mut per_diff: Vec<DiffHashes> = Vec::with_capacity(diffs.len());
		let mut fold = |hashes: &[Hash128], touched: &mut Vec<Hash128>| {
			for &hash in hashes {
				if seen.insert(hash) {
					touched.push(hash);
				}
			}
		};
		for diff in diffs {
			let (pre_hashes, post_hashes) = match diff {
				Diff::Insert {
					post,
					..
				} => (Vec::new(), self.compute_hashes(post)?),
				Diff::Update {
					pre,
					post,
					..
				} => (self.compute_hashes(pre)?, self.compute_hashes(post)?),
				Diff::Remove {
					pre,
					..
				} => (self.compute_hashes(pre)?, Vec::new()),
			};
			fold(&pre_hashes, &mut touched);
			fold(&post_hashes, &mut touched);
			per_diff.push((pre_hashes, post_hashes));
		}
		Ok((touched, per_diff))
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
		let change = decode_change(host, change)?;
		let plan = &self.plan;
		let operator_id = plan.operator;
		let (ordered, diff_hashes) = plan.batch_hashes(&change.diffs)?;

		let mut groups: HashMap<Hash128, GroupId> = HashMap::with_capacity(ordered.len());
		let mut keys = Vec::with_capacity(ordered.len() + 1);
		keys.push(DistinctPlan::layout_storage_key());
		for hash in ordered.iter() {
			let group = GroupId::hashed(*hash);
			groups.insert(*hash, group);
			keys.push(DistinctPlan::entry_key(group));
		}
		let mut rows = host.state_get_many(&keys)?.into_iter();

		let mut state = DistinctState {
			entries: IndexMap::new(),
			layout: DistinctPlan::decode_layout(rows.next().flatten())?,
			dirty: HashSet::new(),
			layout_changed: false,
			withdrawn: IndexSet::new(),
		};

		for (hash, row) in ordered.iter().zip(rows) {
			match DistinctPlan::decode_entry(row)? {
				LoadedEntry::Present(entry) => {
					state.entries.insert(*hash, entry);
				}
				LoadedEntry::Empty => {
					state.dirty.insert(*hash);
				}
				LoadedEntry::Absent => {}
			}
		}

		let mut result = Vec::new();
		for (diff, (pre_hashes, post_hashes)) in change.diffs.into_iter().zip(diff_hashes) {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let insert_result =
						plan.process_insert(host, &mut state, &groups, &post, &post_hashes)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result = plan.process_update(
						host,
						&mut state,
						&groups,
						&pre,
						&post,
						(&pre_hashes, &post_hashes),
					)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
					..
				} => {
					let remove_result =
						plan.process_remove(host, &mut state, &groups, &pre, &pre_hashes)?;
					result.extend(remove_result);
				}
			}
		}

		plan.persist(host, &mut state, &groups)?;

		Ok(Change::from_flow(operator_id, change.version, result, change.changed_at))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}
