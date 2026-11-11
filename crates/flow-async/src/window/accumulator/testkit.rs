// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::accumulator::{
	AccumulatorEvents, AccumulatorOp, Contribution, Contributions, WindowAccumulator, invertible::ordf64::OrdF64,
	one,
};

pub(crate) fn assert_add_remove_is_inverse<A: WindowAccumulator>(initial: &[Contribution<A>], probe: Contribution<A>) {
	let mut accumulator = A::default();
	for c in initial {
		accumulator.add(one::<A::Contributions>(c.clone()).slice());
	}
	let before = accumulator.finalize();
	let probe = one::<A::Contributions>(probe);
	accumulator.add(probe.slice());
	accumulator.remove(probe.slice());
	assert_eq!(accumulator.finalize(), before, "add then remove must restore finalize()");
}

pub(crate) fn assert_order_independent<A>(contributions: &[Contribution<A>])
where
	A: WindowAccumulator,
{
	let mut forward = A::default();
	for c in contributions {
		forward.add(one::<A::Contributions>(c.clone()).slice());
	}
	let mut backward = A::default();
	for c in contributions.iter().rev() {
		backward.add(one::<A::Contributions>(c.clone()).slice());
	}
	assert_eq!(forward.finalize(), backward.finalize(), "finalize() must be order-independent");
}

pub(crate) fn of64(v: f64) -> OrdF64 {
	OrdF64::new(v).expect("not nan")
}

pub(crate) enum Op<Contribution> {
	Add(Contribution),
	Remove(Contribution),
}

pub(crate) fn drive<A: WindowAccumulator>(accumulator: &mut A, ops: &[Op<Contribution<A>>]) {
	for op in ops {
		match op {
			Op::Add(c) => accumulator.add(one::<A::Contributions>(c.clone()).slice()),
			Op::Remove(c) => accumulator.remove(one::<A::Contributions>(c.clone()).slice()),
		}
	}
}

pub(crate) fn assert_arms_agree<A: WindowAccumulator>(
	mut sealed: A,
	mut unsealed: A,
	ops: &[Op<Contribution<A>>],
	why: &str,
) {
	drive(&mut sealed, ops);
	drive(&mut unsealed, ops);
	assert_eq!(sealed.finalize(), unsealed.finalize(), "{why}");
}

pub(crate) fn events<X: Contributions>(
	ops: impl IntoIterator<Item = (AccumulatorOp, X::Item)>,
) -> AccumulatorEvents<X> {
	let mut events = AccumulatorEvents::new();
	for (op, contribution) in ops {
		events.push(op, contribution);
	}
	events
}
