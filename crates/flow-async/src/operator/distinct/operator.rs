// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	mem::take,
	sync::Arc,
};

use arrow_schema::SchemaRef;
use indexmap::{IndexMap, IndexSet};
use reifydb_codec::{
	key::{decode_u64_asc, encode_u64_asc},
	row::{
		operator::state::{OperatorState, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey},
};
use reifydb_evaluate::lower::{LoweredExpr, compile_check};
use reifydb_flow::{context::FlowContext, operator::source::decode_change};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, error::Error, util::hash::Hash128, value::row_number::RowNumber};
use tracing::instrument;

use crate::{
	error::FlowStateError,
	operator::{
		HostOperator,
		distinct::state::{DistinctEntry, DistinctLayout, DistinctState, SerializedRow},
		drops::SealedDrops,
		host::HostContext,
		state::store::sweep_all,
	},
};

const DROP_REASON: &str = "removes whose distinct entry was reclaimed";

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

type DiffHashes = (Vec<Hash128>, Vec<Hash128>);

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
	pub(super) fn row_key(group: GroupId, row: RowNumber) -> GroupStateKey {
		OperatorStateKey::inner_encoded(group, KeyspaceId::DISTINCT_ENTRY, encode_u64_asc(row.0).to_vec())
	}

	pub(super) fn layout_storage_key() -> GroupStateKey {
		GroupStateKey::root(KeyspaceId::DISTINCT_LAYOUT, vec![])
	}

	fn load_entries(
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		ordered: &[Hash128],
	) -> Result<()> {
		if ordered.is_empty() {
			return Ok(());
		}
		let hashes: HashMap<GroupId, Hash128> = groups.iter().map(|(hash, group)| (*group, *hash)).collect();
		let group_ids: Vec<GroupId> = ordered.iter().map(|hash| groups[hash]).collect();
		for (key, row) in sweep_all(host, &group_ids, KeyspaceMask::of([KeyspaceId::DISTINCT_ENTRY]))? {
			let Some((group, _, suffix)) = OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
			else {
				return internal_err!("a distinct row carries no group");
			};
			let (Some(hash), Ok(order)) = (hashes.get(&group), <[u8; 8]>::try_from(suffix)) else {
				return internal_err!("a distinct row carries no row number");
			};
			let number = RowNumber(decode_u64_asc(order));
			let serialized: SerializedRow = decode(&row).map_err(|e| {
				Error::from(FlowStateError::Decode {
					state: "SerializedRow",
					cause: e.to_string(),
				})
			})?;
			host.state_classify(&key, Some(row.byte_size()));
			state.loaded.insert((*hash, number));
			state.entries
				.entry(*hash)
				.or_insert_with(|| DistinctEntry {
					rows: BTreeMap::new(),
				})
				.rows
				.insert(number, serialized);
		}
		Ok(())
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
		let dirty: Vec<(Hash128, RowNumber)> = state.dirty.drain().collect();
		let mut sets = Vec::new();
		let mut removes = Vec::new();
		for (hash, number) in dirty {
			let key = Self::row_key(groups[&hash], number);
			let loaded = state.loaded.contains(&(hash, number));
			match state.entries.get(&hash).and_then(|entry| entry.rows.get(&number)) {
				Some(serialized) => {
					let row = serialized.encode_state().map_err(|e| {
						Error::from(FlowStateError::Encode {
							state: "SerializedRow",
							cause: e.to_string(),
						})
					})?;
					if !loaded {
						host.state_classify(&key, None);
					}
					sets.push((key, row));
				}
				None if loaded => removes.push(key),
				None => {}
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

		let groups: HashMap<Hash128, GroupId> =
			ordered.iter().map(|hash| (*hash, GroupId::hashed(*hash))).collect();
		let layout_key = DistinctPlan::layout_storage_key();
		let layout = host.state_get(&layout_key)?;
		host.state_classify(&layout_key, layout.as_ref().map(EncodedPodRow::byte_size));

		let mut state = DistinctState {
			entries: IndexMap::new(),
			layout: DistinctPlan::decode_layout(layout)?,
			dirty: HashSet::new(),
			loaded: HashSet::new(),
			layout_changed: false,
			withdrawn: IndexSet::new(),
		};
		DistinctPlan::load_entries(host, &mut state, &groups, &ordered)?;

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
