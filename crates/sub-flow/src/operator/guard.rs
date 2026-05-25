// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::process::abort;

use Diff::*;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::interface::{
	catalog::flow::FlowNodeId,
	change::{Change, Diff},
};
use tracing::error;

#[derive(Debug, PartialEq, Eq)]
pub enum CapabilityViolation {
	Apply {
		kind: &'static str,
		missing: OperatorCapability,
	},
	Tick,
}

pub fn check_apply(caps: &[OperatorCapability], change: &Change) -> Result<(), CapabilityViolation> {
	for diff in change.diffs.iter() {
		match diff {
			Insert {
				..
			} if !caps.contains(&OperatorCapability::Insert) => {
				return Err(CapabilityViolation::Apply {
					kind: "insert",
					missing: OperatorCapability::Insert,
				});
			}
			Update {
				..
			} if !caps.contains(&OperatorCapability::Update) => {
				return Err(CapabilityViolation::Apply {
					kind: "update",
					missing: OperatorCapability::Update,
				});
			}
			Remove {
				..
			} if !caps.contains(&OperatorCapability::Delete) => {
				return Err(CapabilityViolation::Apply {
					kind: "remove",
					missing: OperatorCapability::Delete,
				});
			}
			_ => {}
		}
	}
	Ok(())
}

pub fn check_tick(caps: &[OperatorCapability]) -> Result<(), CapabilityViolation> {
	if caps.contains(&OperatorCapability::Tick) {
		Ok(())
	} else {
		Err(CapabilityViolation::Tick)
	}
}

pub fn enforce_apply_capabilities(operator_id: FlowNodeId, caps: &[OperatorCapability], change: &Change) {
	if let Err(v) = check_apply(caps, change) {
		match v {
			CapabilityViolation::Apply {
				kind,
				missing,
			} => {
				error!(
					operator_id = operator_id.0,
					kind = kind,
					missing_capability = ?missing,
					"operator received {} diff but does not declare the corresponding capability ({:?}); the operator's author did not opt into this change kind. Aborting to prevent undefined behavior.",
					kind,
					missing,
				);
				abort();
			}
			_ => unreachable!(),
		}
	}
}

pub fn enforce_tick_capability(operator_id: FlowNodeId, caps: &[OperatorCapability]) {
	if check_tick(caps).is_err() {
		error!(
			operator_id = operator_id.0,
			"operator received a tick but does not declare the Tick capability. Aborting.",
		);
		abort();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::flow::FlowNodeId,
			change::{Change, Diff, Diffs},
		},
		value::column::columns::Columns,
	};
	use reifydb_type::value::datetime::DateTime;

	use super::*;

	fn change(diffs: Vec<Diff>) -> Change {
		let mut sv = Diffs::new();
		for d in diffs {
			sv.push(d);
		}
		Change::from_flow(FlowNodeId(1), CommitVersion(0), sv, DateTime::default())
	}

	fn insert() -> Diff {
		Diff::insert(Columns::empty())
	}

	fn update() -> Diff {
		Diff::update(Columns::empty(), Columns::empty())
	}

	fn remove() -> Diff {
		Diff::remove(Columns::empty())
	}

	#[test]
	fn insert_only_caps_reject_update_diff() {
		let c = change(vec![update()]);
		assert_eq!(
			check_apply(&[OperatorCapability::Insert], &c),
			Err(CapabilityViolation::Apply {
				kind: "update",
				missing: OperatorCapability::Update,
			})
		);
	}

	#[test]
	fn insert_only_caps_reject_remove_diff() {
		let c = change(vec![remove()]);
		assert_eq!(
			check_apply(&[OperatorCapability::Insert], &c),
			Err(CapabilityViolation::Apply {
				kind: "remove",
				missing: OperatorCapability::Delete,
			})
		);
	}

	#[test]
	fn update_only_caps_reject_insert_diff() {
		let c = change(vec![insert()]);
		assert_eq!(
			check_apply(&[OperatorCapability::Update], &c),
			Err(CapabilityViolation::Apply {
				kind: "insert",
				missing: OperatorCapability::Insert,
			})
		);
	}

	#[test]
	fn zero_caps_reject_insert_diff() {
		let c = change(vec![insert()]);
		assert_eq!(
			check_apply(&[], &c),
			Err(CapabilityViolation::Apply {
				kind: "insert",
				missing: OperatorCapability::Insert,
			})
		);
	}

	#[test]
	fn all_standard_caps_accept_all_diff_kinds() {
		let c = change(vec![insert(), update(), remove()]);
		assert_eq!(check_apply(OperatorCapability::STANDARD, &c), Ok(()));
	}

	#[test]
	fn empty_diffs_accepted_with_zero_caps() {
		let c = change(vec![]);
		assert_eq!(check_apply(&[], &c), Ok(()));
	}

	#[test]
	fn check_tick_requires_tick_bit() {
		assert_eq!(check_tick(&[]), Err(CapabilityViolation::Tick));
		assert_eq!(check_tick(OperatorCapability::STANDARD), Err(CapabilityViolation::Tick));
		assert_eq!(check_tick(&[OperatorCapability::Tick]), Ok(()));
		assert_eq!(check_tick(OperatorCapability::STANDARD_WITH_TICK), Ok(()));
	}

	#[test]
	fn check_apply_first_violation_in_diff_list_wins() {
		let c = change(vec![insert(), update(), remove()]);
		assert_eq!(
			check_apply(&[OperatorCapability::Insert], &c),
			Err(CapabilityViolation::Apply {
				kind: "update",
				missing: OperatorCapability::Update,
			})
		);
	}
}
