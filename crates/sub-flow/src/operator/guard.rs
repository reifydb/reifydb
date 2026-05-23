// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::process::abort;

use Diff::*;
use reifydb_abi::operator::capabilities::{
	CAPABILITY_DELETE, CAPABILITY_INSERT, CAPABILITY_TICK, CAPABILITY_UPDATE, has_capability,
};
use reifydb_core::interface::{
	catalog::flow::FlowNodeId,
	change::{Change, Diff},
};
use tracing::error;

#[derive(Debug, PartialEq, Eq)]
pub enum CapabilityViolation {
	Apply {
		kind: &'static str,
		missing_bit: u32,
	},
	Tick,
}

pub fn check_apply(caps: u32, change: &Change) -> Result<(), CapabilityViolation> {
	for diff in change.diffs.iter() {
		match diff {
			Insert {
				..
			} if !has_capability(caps, CAPABILITY_INSERT) => {
				return Err(CapabilityViolation::Apply {
					kind: "insert",
					missing_bit: CAPABILITY_INSERT,
				});
			}
			Update {
				..
			} if !has_capability(caps, CAPABILITY_UPDATE) => {
				return Err(CapabilityViolation::Apply {
					kind: "update",
					missing_bit: CAPABILITY_UPDATE,
				});
			}
			Remove {
				..
			} if !has_capability(caps, CAPABILITY_DELETE) => {
				return Err(CapabilityViolation::Apply {
					kind: "remove",
					missing_bit: CAPABILITY_DELETE,
				});
			}
			_ => {}
		}
	}
	Ok(())
}

pub fn check_tick(caps: u32) -> Result<(), CapabilityViolation> {
	if has_capability(caps, CAPABILITY_TICK) {
		Ok(())
	} else {
		Err(CapabilityViolation::Tick)
	}
}

pub fn enforce_apply_capabilities(operator_id: FlowNodeId, caps: u32, change: &Change) {
	if let Err(v) = check_apply(caps, change) {
		match v {
			CapabilityViolation::Apply {
				kind,
				missing_bit,
			} => {
				error!(
					operator_id = operator_id.0,
					kind = kind,
					missing_capability_bit = missing_bit,
					"operator received {} diff but does not declare the corresponding capability bit (0x{:08x}); the operator's author did not opt into this change kind. Aborting to prevent undefined behavior.",
					kind,
					missing_bit,
				);
				abort();
			}
			_ => unreachable!(),
		}
	}
}

pub fn enforce_tick_capability(operator_id: FlowNodeId, caps: u32) {
	if check_tick(caps).is_err() {
		error!(
			operator_id = operator_id.0,
			"operator received a tick but does not declare CAPABILITY_TICK. Aborting.",
		);
		abort();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
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
			check_apply(CAPABILITY_INSERT, &c),
			Err(CapabilityViolation::Apply {
				kind: "update",
				missing_bit: CAPABILITY_UPDATE,
			})
		);
	}

	#[test]
	fn insert_only_caps_reject_remove_diff() {
		let c = change(vec![remove()]);
		assert_eq!(
			check_apply(CAPABILITY_INSERT, &c),
			Err(CapabilityViolation::Apply {
				kind: "remove",
				missing_bit: CAPABILITY_DELETE,
			})
		);
	}

	#[test]
	fn update_only_caps_reject_insert_diff() {
		let c = change(vec![insert()]);
		assert_eq!(
			check_apply(CAPABILITY_UPDATE, &c),
			Err(CapabilityViolation::Apply {
				kind: "insert",
				missing_bit: CAPABILITY_INSERT,
			})
		);
	}

	#[test]
	fn zero_caps_reject_insert_diff() {
		let c = change(vec![insert()]);
		assert_eq!(
			check_apply(0, &c),
			Err(CapabilityViolation::Apply {
				kind: "insert",
				missing_bit: CAPABILITY_INSERT,
			})
		);
	}

	#[test]
	fn all_standard_caps_accept_all_diff_kinds() {
		let c = change(vec![insert(), update(), remove()]);
		assert_eq!(check_apply(CAPABILITY_ALL_STANDARD, &c), Ok(()));
	}

	#[test]
	fn empty_diffs_accepted_with_zero_caps() {
		let c = change(vec![]);
		assert_eq!(check_apply(0, &c), Ok(()));
	}

	#[test]
	fn check_tick_requires_tick_bit() {
		assert_eq!(check_tick(0), Err(CapabilityViolation::Tick));
		assert_eq!(check_tick(CAPABILITY_ALL_STANDARD), Err(CapabilityViolation::Tick));
		assert_eq!(check_tick(CAPABILITY_TICK), Ok(()));
		assert_eq!(check_tick(CAPABILITY_ALL_STANDARD | CAPABILITY_TICK), Ok(()));
	}

	#[test]
	fn check_apply_first_violation_in_diff_list_wins() {
		let c = change(vec![insert(), update(), remove()]);
		assert_eq!(
			check_apply(CAPABILITY_INSERT, &c),
			Err(CapabilityViolation::Apply {
				kind: "update",
				missing_bit: CAPABILITY_UPDATE,
			})
		);
	}
}
