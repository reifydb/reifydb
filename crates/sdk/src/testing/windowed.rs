// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter, Write};

use crate::operator::windowed::TumblingOperator;

pub struct ContractRow<A: TumblingOperator> {
	pub group: A::GroupKey,
	pub slot: A::SlotKey,
	pub input: A::SlotInput,
}

impl<A: TumblingOperator> Clone for ContractRow<A> {
	fn clone(&self) -> Self {
		Self {
			group: self.group.clone(),
			slot: self.slot,
			input: self.input.clone(),
		}
	}
}

impl<A: TumblingOperator> Debug for ContractRow<A> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ContractRow")
			.field("group", &self.group)
			.field("slot", &self.slot)
			.field("input", &self.input)
			.finish()
	}
}

#[derive(Clone, Copy, Debug)]
pub enum RowKind {
	Insert,
	Update,
	Remove,
}

pub struct ContractEvent<A: TumblingOperator> {
	pub kind: RowKind,
	pub row: ContractRow<A>,
}

impl<A: TumblingOperator> Clone for ContractEvent<A> {
	fn clone(&self) -> Self {
		Self {
			kind: self.kind,
			row: self.row.clone(),
		}
	}
}

impl<A: TumblingOperator> Debug for ContractEvent<A> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ContractEvent").field("kind", &self.kind).field("row", &self.row).finish()
	}
}

pub struct ContractCase<A: TumblingOperator> {
	pub events: Vec<ContractEvent<A>>,
}

impl<A: TumblingOperator> Default for ContractCase<A> {
	fn default() -> Self {
		Self {
			events: Vec::new(),
		}
	}
}

impl<A: TumblingOperator> Clone for ContractCase<A> {
	fn clone(&self) -> Self {
		Self {
			events: self.events.clone(),
		}
	}
}

impl<A: TumblingOperator> ContractCase<A> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn push(mut self, kind: RowKind, group: A::GroupKey, slot: A::SlotKey, input: A::SlotInput) -> Self {
		self.events.push(ContractEvent {
			kind,
			row: ContractRow {
				group,
				slot,
				input,
			},
		});
		self
	}
}

pub fn assert_tumbling_contract<A: TumblingOperator>(agg: &A, case: ContractCase<A>) {
	let mut report = Report::new();
	check_window_boundary_half_open(agg, &case, &mut report);
	if let Some(msg) = report.into_failure() {
		panic!("\ntumbling_contract violation:\n{msg}");
	}
}

fn check_window_boundary_half_open<A: TumblingOperator>(agg: &A, case: &ContractCase<A>, report: &mut Report) {
	for ev in &case.events {
		let span = agg.window_for(ev.row.slot);
		if !span.contains(ev.row.slot) {
			report.fail(
				"window_for(slot) returned a span that does not contain slot",
				"TumblingOperator::window_for must return a span s.t. s.contains(slot) \
				 is true. The span itself is half-open [start, end); see WindowSpan docs.",
			);
			return;
		}
		if span.contains(span.end) {
			report.fail(
				"WindowSpan boundary is not half-open",
				"WindowSpan::contains must be false for span.end (the boundary slot belongs \
				 to the next window). This usually means a hand-rolled membership check \
				 used <= instead of <.",
			);
			return;
		}
	}
}

#[derive(Default)]
struct Report {
	failures: Vec<(String, String)>,
}

impl Report {
	fn new() -> Self {
		Self::default()
	}

	fn fail(&mut self, headline: &str, detail: &str) {
		self.failures.push((headline.to_string(), detail.to_string()));
	}

	fn into_failure(self) -> Option<String> {
		if self.failures.is_empty() {
			return None;
		}
		let mut buf = String::new();
		for (i, (headline, detail)) in self.failures.iter().enumerate() {
			let _ = write!(buf, "  [{i}] {headline}\n      {detail}\n");
		}
		Some(buf)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use serde::{Deserialize, Serialize};

	use super::*;
	use crate::operator::{view::RowView, windowed::span::WindowSpan};

	#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
	struct Trade {
		size: f64,
	}

	#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
	struct VolSlot {
		size: f64,
	}

	#[derive(Clone, Copy, Debug, PartialEq)]
	struct VolOut {
		volume: f64,
	}

	struct GoodVolume;

	impl TumblingOperator for GoodVolume {
		type GroupKey = String;
		type SlotKey = u64;
		type SlotInput = Trade;
		type SlotContribution = VolSlot;
		type Output = VolOut;

		fn extract(&self, _row: &impl RowView) -> Option<(Self::GroupKey, Self::SlotKey, Self::SlotInput)> {
			None
		}

		fn fold_into_slot(&self, _prev: Option<&VolSlot>, input: &Trade) -> VolSlot {
			VolSlot {
				size: input.size,
			}
		}

		fn combine(
			&self,
			_group: &String,
			_span: WindowSpan<u64>,
			slots: &BTreeMap<u64, VolSlot>,
			_prev_window_close: Option<&VolSlot>,
		) -> Option<VolOut> {
			(!slots.is_empty()).then(|| VolOut {
				volume: slots.values().map(|s| s.size).sum(),
			})
		}

		fn window_for(&self, slot: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(slot, 60)
		}
	}

	#[test]
	fn good_aggregator_passes() {
		let agg = GoodVolume;
		let case = ContractCase::<GoodVolume>::new()
			.push(
				RowKind::Insert,
				"BTC".to_string(),
				0,
				Trade {
					size: 1.0,
				},
			)
			.push(
				RowKind::Insert,
				"BTC".to_string(),
				59,
				Trade {
					size: 2.0,
				},
			)
			.push(
				RowKind::Insert,
				"BTC".to_string(),
				60,
				Trade {
					size: 3.0,
				},
			);
		assert_tumbling_contract(&agg, case);
	}

	/// Negative control: `window_for` claims slot 60 belongs to the
	/// previous window `[0, 60)`, which excludes 60.
	struct InclusiveBoundary;

	impl TumblingOperator for InclusiveBoundary {
		type GroupKey = String;
		type SlotKey = u64;
		type SlotInput = Trade;
		type SlotContribution = VolSlot;
		type Output = VolOut;

		fn extract(&self, _row: &impl RowView) -> Option<(Self::GroupKey, Self::SlotKey, Self::SlotInput)> {
			None
		}

		fn fold_into_slot(&self, _prev: Option<&VolSlot>, input: &Trade) -> VolSlot {
			VolSlot {
				size: input.size,
			}
		}

		fn combine(
			&self,
			_group: &String,
			_span: WindowSpan<u64>,
			slots: &BTreeMap<u64, VolSlot>,
			_prev_window_close: Option<&VolSlot>,
		) -> Option<VolOut> {
			(!slots.is_empty()).then(|| VolOut {
				volume: slots.values().map(|s| s.size).sum(),
			})
		}

		fn window_for(&self, slot: u64) -> WindowSpan<u64> {
			let aligned = slot - (slot % 60);
			if slot == aligned && slot >= 60 {
				WindowSpan::new(aligned - 60, aligned)
			} else {
				WindowSpan::new(aligned, aligned + 60)
			}
		}
	}

	#[test]
	#[should_panic(expected = "window_for(slot) returned a span that does not contain slot")]
	fn inclusive_boundary_is_caught() {
		let agg = InclusiveBoundary;
		let case = ContractCase::<InclusiveBoundary>::new().push(
			RowKind::Insert,
			"BTC".to_string(),
			60,
			Trade {
				size: 1.0,
			},
		);
		assert_tumbling_contract(&agg, case);
	}
}
