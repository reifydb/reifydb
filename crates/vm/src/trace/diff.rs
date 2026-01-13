// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Delta computation between VM state snapshots.

use super::entry::{CallFrameSnapshot, OperandSnapshot, ScopeSnapshot, StateChange, StateSnapshot};

/// Compute the changes between two state snapshots.
pub fn compute_diff(before: &StateSnapshot, after: &StateSnapshot) -> Vec<StateChange> {
	let mut changes = Vec::new();

	// Compare operand stacks
	diff_operand_stacks(&before.operand_stack, &after.operand_stack, &mut changes);

	// Compare pipeline stacks
	diff_pipeline_stacks(&before.pipeline_stack, &after.pipeline_stack, &mut changes);

	// Compare scopes
	diff_scopes(&before.scopes, &after.scopes, &mut changes);

	// Compare call stacks
	diff_call_stacks(&before.call_stack, &after.call_stack, &mut changes);

	changes
}

/// Diff operand stacks.
fn diff_operand_stacks(before: &[OperandSnapshot], after: &[OperandSnapshot], changes: &mut Vec<StateChange>) {
	// Items that were popped (in before but not in after at same position, or beyond after's length)
	for i in (after.len()..before.len()).rev() {
		changes.push(StateChange::StackPop {
			index: i,
			value: before[i].clone(),
		});
	}

	// Items that were pushed (in after but not in before at same position, or beyond before's length)
	for i in before.len()..after.len() {
		changes.push(StateChange::StackPush {
			index: i,
			value: after[i].clone(),
		});
	}

	// For now we don't track in-place modifications to stack elements
	// (This is rare in practice - usually values are pushed/popped)
}

/// Diff pipeline stacks.
fn diff_pipeline_stacks(before: &[String], after: &[String], changes: &mut Vec<StateChange>) {
	// Handle pops
	for i in (after.len()..before.len()).rev() {
		changes.push(StateChange::PipelinePop {
			index: i,
			desc: before[i].clone(),
		});
	}

	// Handle pushes
	for i in before.len()..after.len() {
		changes.push(StateChange::PipelinePush {
			index: i,
			desc: after[i].clone(),
		});
	}

	// Handle modifications (same index but different description)
	let common_len = before.len().min(after.len());
	for i in 0..common_len {
		if before[i] != after[i] {
			changes.push(StateChange::PipelineModify {
				index: i,
				from: before[i].clone(),
				to: after[i].clone(),
			});
		}
	}
}

/// Diff scope chains.
fn diff_scopes(before: &[ScopeSnapshot], after: &[ScopeSnapshot], changes: &mut Vec<StateChange>) {
	// Handle scope pops
	for i in (after.len()..before.len()).rev() {
		changes.push(StateChange::ScopePop {
			depth: before[i].depth,
		});
	}

	// Handle scope pushes
	for i in before.len()..after.len() {
		changes.push(StateChange::ScopePush {
			depth: after[i].depth,
		});
	}

	// For each common scope, diff the variables
	let common_len = before.len().min(after.len());
	for i in 0..common_len {
		diff_scope_variables(&before[i], &after[i], changes);
	}
}

/// Diff variables within a single scope.
fn diff_scope_variables(before: &ScopeSnapshot, after: &ScopeSnapshot, changes: &mut Vec<StateChange>) {
	use std::collections::HashMap;

	let before_vars: HashMap<&str, &OperandSnapshot> =
		before.variables.iter().map(|(k, v)| (k.as_str(), v)).collect();

	let after_vars: HashMap<&str, &OperandSnapshot> =
		after.variables.iter().map(|(k, v)| (k.as_str(), v)).collect();

	// Variables removed
	for (name, value) in &before_vars {
		if !after_vars.contains_key(name) {
			changes.push(StateChange::VarRemove {
				scope_depth: before.depth,
				name: name.to_string(),
				value: (*value).clone(),
			});
		}
	}

	// Variables added or changed
	for (name, value) in &after_vars {
		match before_vars.get(name) {
			None => {
				// New variable
				changes.push(StateChange::VarSet {
					scope_depth: after.depth,
					name: name.to_string(),
					value: (*value).clone(),
				});
			}
			Some(old_value) => {
				// Check if value changed (simplified comparison - just check if they're different debug
				// strings) TODO: Implement proper PartialEq for OperandSnapshot if needed
				if format!("{:?}", old_value) != format!("{:?}", value) {
					// Variable was updated - show as remove + set
					changes.push(StateChange::VarRemove {
						scope_depth: before.depth,
						name: name.to_string(),
						value: (*old_value).clone(),
					});
					changes.push(StateChange::VarSet {
						scope_depth: after.depth,
						name: name.to_string(),
						value: (*value).clone(),
					});
				}
			}
		}
	}
}

/// Diff call stacks.
fn diff_call_stacks(before: &[CallFrameSnapshot], after: &[CallFrameSnapshot], changes: &mut Vec<StateChange>) {
	// Handle pops
	for i in (after.len()..before.len()).rev() {
		changes.push(StateChange::CallPop {
			frame: before[i].clone(),
		});
	}

	// Handle pushes
	for i in before.len()..after.len() {
		changes.push(StateChange::CallPush {
			frame: after[i].clone(),
		});
	}
}
