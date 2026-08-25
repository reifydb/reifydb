// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_value::{Result, error::Error, value::row_number::RowNumber};

use crate::error::FlowGraphError;

pub const MAX_LANES: u64 = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendLanes {
	operator: OperatorId,
	bits: u32,
	stamps: [Option<u64>; 2],
}

impl AppendLanes {
	pub fn new(operator: OperatorId, bits: u32, stamps: [Option<u64>; 2]) -> Self {
		Self {
			operator,
			bits,
			stamps,
		}
	}

	pub fn bits(&self) -> u32 {
		self.bits
	}

	pub fn stamps(&self) -> &[Option<u64>; 2] {
		&self.stamps
	}

	pub fn stamp(&self, input: usize, source: RowNumber) -> RowNumber {
		match self.stamps.get(input).copied().flatten() {
			None => source,
			Some(lane) => {
				assert!(
					self.bits > 0 && source.0 >> (64 - self.bits) == 0,
					"append operator {:?} cannot widen source row {} by {} lane bits without \
					 dropping high bits, which would alias two source rows onto one output row",
					self.operator,
					source.0,
					self.bits
				);
				RowNumber((source.0 << self.bits) | lane)
			}
		}
	}
}

fn preserves_row_numbers(ty: &OperatorDef) -> bool {
	matches!(
		ty,
		OperatorDef::Filter { .. }
			| OperatorDef::Map { .. }
			| OperatorDef::Extend { .. }
			| OperatorDef::Gate { .. }
			| OperatorDef::Take { .. }
			| OperatorDef::Sort { .. }
	)
}

fn is_append(dag: &FlowDag, node: OperatorId) -> bool {
	matches!(dag.get_operator(&node).map(|n| &n.ty), Some(OperatorDef::Append { .. }))
}

fn resolve_down(dag: &FlowDag, mut node: OperatorId) -> Result<OperatorId> {
	loop {
		let found = dag.get_operator(&node).ok_or_else(|| missing(node))?;
		if !preserves_row_numbers(&found.ty) {
			return Ok(node);
		}
		let Some(next) = found.inputs.first().copied() else {
			return Ok(node);
		};
		node = next;
	}
}

fn chain_parent(dag: &FlowDag, node: OperatorId) -> Result<Option<OperatorId>> {
	let mut frontier = vec![node];
	let mut parent = None;
	while let Some(current) = frontier.pop() {
		let found = dag.get_operator(&current).ok_or_else(|| missing(current))?;
		for output in &found.outputs {
			let target = dag.get_operator(output).ok_or_else(|| missing(*output))?;
			if preserves_row_numbers(&target.ty) {
				frontier.push(*output);
				continue;
			}
			if !matches!(target.ty, OperatorDef::Append { .. }) {
				continue;
			}
			if parent.is_some_and(|existing| existing != *output) {
				return Err(Error::from(FlowGraphError::NodeInputArity {
					operator: "Append",
					expected: "one append consumer per append chain",
					found: 2,
				}));
			}
			parent = Some(*output);
		}
	}
	Ok(parent)
}

fn chain_root(dag: &FlowDag, node: OperatorId) -> Result<OperatorId> {
	let mut current = node;
	while let Some(parent) = chain_parent(dag, current)? {
		current = parent;
	}
	Ok(current)
}

fn count_leaves(dag: &FlowDag, node: OperatorId) -> Result<u64> {
	if !is_append(dag, node) {
		return Ok(1);
	}
	let found = dag.get_operator(&node).ok_or_else(|| missing(node))?;
	let mut total = 0;
	for input in &found.inputs {
		total += count_leaves(dag, resolve_down(dag, *input)?)?;
	}
	Ok(total)
}

fn walk(
	dag: &FlowDag,
	node: OperatorId,
	next_lane: &mut u64,
	out: &mut HashMap<OperatorId, [Option<u64>; 2]>,
) -> Result<()> {
	let found = dag.get_operator(&node).ok_or_else(|| missing(node))?;
	if found.inputs.len() != 2 {
		return Err(Error::from(FlowGraphError::NodeInputArity {
			operator: "Append",
			expected: "exactly 2",
			found: found.inputs.len(),
		}));
	}
	let mut stamps = [None, None];
	for (index, input) in found.inputs.iter().enumerate() {
		let resolved = resolve_down(dag, *input)?;
		if is_append(dag, resolved) {
			walk(dag, resolved, next_lane, out)?;
			continue;
		}
		stamps[index] = Some(*next_lane);
		*next_lane += 1;
	}
	out.insert(node, stamps);
	Ok(())
}

fn missing(node: OperatorId) -> Error {
	Error::from(FlowGraphError::ParentOperatorNotFound {
		input: format!("{:?}", node),
	})
}

pub fn assign_lanes(dag: &FlowDag, node: OperatorId) -> Result<AppendLanes> {
	let root = chain_root(dag, node)?;
	let leaves = count_leaves(dag, root)?;
	if leaves > MAX_LANES {
		return Err(Error::from(FlowGraphError::NodeInputArity {
			operator: "Append",
			expected: "at most 256 branches per append chain",
			found: leaves as usize,
		}));
	}
	let bits = leaves.next_power_of_two().trailing_zeros().max(1);

	let mut lanes = HashMap::new();
	let mut next_lane = 0;
	walk(dag, root, &mut next_lane, &mut lanes)?;

	let stamps = lanes.get(&node).copied().ok_or_else(|| missing(node))?;
	Ok(AppendLanes::new(node, bits, stamps))
}
