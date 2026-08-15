// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::accumulator::{WindowAccumulator, invertible::ordf64::OrdF64};

pub(crate) fn assert_add_remove_is_inverse<A: WindowAccumulator>(initial: &[A::Contribution], probe: A::Contribution) {
	let mut accumulator = A::default();
	for c in initial {
		accumulator.add(c);
	}
	let before = accumulator.finalize();
	accumulator.add(&probe);
	accumulator.remove(&probe);
	assert_eq!(accumulator.finalize(), before, "add then remove must restore finalize()");
}

pub(crate) fn assert_order_independent<A>(contributions: &[A::Contribution])
where
	A: WindowAccumulator,
{
	let mut forward = A::default();
	for c in contributions {
		forward.add(c);
	}
	let mut backward = A::default();
	for c in contributions.iter().rev() {
		backward.add(c);
	}
	assert_eq!(forward.finalize(), backward.finalize(), "finalize() must be order-independent");
}

pub(crate) fn of64(v: f64) -> OrdF64 {
	OrdF64::new(v).expect("not nan")
}

pub(crate) enum Op<C> {
	Add(C),
	Remove(C),
}

pub(crate) fn drive<A: WindowAccumulator>(accumulator: &mut A, ops: &[Op<A::Contribution>]) {
	for op in ops {
		match op {
			Op::Add(c) => accumulator.add(c),
			Op::Remove(c) => accumulator.remove(c),
		}
	}
}

pub(crate) fn assert_arms_agree<A: WindowAccumulator>(
	mut sealed: A,
	mut unsealed: A,
	ops: &[Op<A::Contribution>],
	why: &str,
) {
	drive(&mut sealed, ops);
	drive(&mut unsealed, ops);
	assert_eq!(sealed.finalize(), unsealed.finalize(), "{why}");
}
