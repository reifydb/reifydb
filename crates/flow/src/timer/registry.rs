// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::catalog::flow::{FlowId, OperatorId};
use reifydb_value::value::datetime::DateTime;

use crate::timer::TimerDue;

#[derive(Default)]
pub struct TimerRegistry {
	flows: HashMap<FlowId, HashMap<OperatorId, DateTime>>,
}

pub struct TimerStage {
	flow: FlowId,
	staged: TimerRegistry,
}

impl TimerStage {
	pub fn due_before(&mut self, armed: Vec<TimerDue>, watermark: DateTime) -> Vec<TimerDue> {
		self.staged.due_before(armed, self.flow, watermark)
	}

	pub fn refresh(&mut self, operator: OperatorId, next: Option<TimerDue>) {
		self.staged.refresh(self.flow, operator, next);
	}
}

impl TimerRegistry {
	pub fn stage(&self, flow: FlowId) -> TimerStage {
		let mut staged = TimerRegistry::default();
		if let Some(operators) = self.flows.get(&flow) {
			staged.flows.insert(flow, operators.clone());
		}
		TimerStage {
			flow,
			staged,
		}
	}

	pub fn apply(&mut self, stage: TimerStage) {
		let TimerStage {
			flow,
			mut staged,
		} = stage;
		match staged.flows.remove(&flow) {
			Some(operators) => {
				self.flows.insert(flow, operators);
			}
			None => {
				self.flows.remove(&flow);
			}
		}
	}

	pub fn due_before(&mut self, armed: Vec<TimerDue>, flow: FlowId, watermark: DateTime) -> Vec<TimerDue> {
		self.fold(flow, armed);
		let Some(operators) = self.flows.get(&flow) else {
			return Vec::new();
		};
		if operators.values().min().is_none_or(|earliest| *earliest > watermark) {
			return Vec::new();
		}
		operators
			.iter()
			.filter(|(_, due)| **due <= watermark)
			.map(|(operator_id, due)| TimerDue {
				operator_id: *operator_id,
				due: *due,
			})
			.collect()
	}

	pub fn refresh(&mut self, flow: FlowId, operator: OperatorId, next: Option<TimerDue>) {
		let Some(operators) = self.flows.get_mut(&flow) else {
			return;
		};
		match next {
			Some(next) => {
				operators.insert(operator, next.due);
			}
			None => {
				operators.remove(&operator);
			}
		}
	}

	pub fn rebuild(&mut self, flow: FlowId, armed: Vec<TimerDue>) {
		self.flows.remove(&flow);
		self.fold(flow, armed);
	}

	pub fn remove_operator(&mut self, flow: FlowId, operator: OperatorId) {
		let Some(operators) = self.flows.get_mut(&flow) else {
			return;
		};
		operators.remove(&operator);
		if operators.is_empty() {
			self.flows.remove(&flow);
		}
	}

	pub fn remove_flow(&mut self, flow: FlowId) {
		self.flows.remove(&flow);
	}

	pub fn clear(&mut self) {
		self.flows.clear();
	}

	fn fold(&mut self, flow: FlowId, armed: Vec<TimerDue>) {
		if armed.is_empty() {
			return;
		}
		let operators = self.flows.entry(flow).or_default();
		for entry in armed {
			operators
				.entry(entry.operator_id)
				.and_modify(|earliest| *earliest = (*earliest).min(entry.due))
				.or_insert(entry.due);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn operator(id: u64) -> OperatorId {
		OperatorId(id)
	}

	fn flow(id: u64) -> FlowId {
		FlowId(id)
	}

	fn armed(operator_id: u64, millis: u64) -> TimerDue {
		TimerDue {
			operator_id: operator(operator_id),
			due: DateTime::from_millis(millis),
		}
	}

	fn entries(registry: &TimerRegistry, flow: FlowId) -> Vec<TimerDue> {
		let Some(operators) = registry.flows.get(&flow) else {
			return Vec::new();
		};
		let mut held: Vec<TimerDue> = operators
			.iter()
			.map(|(operator_id, due)| TimerDue {
				operator_id: *operator_id,
				due: *due,
			})
			.collect();
		held.sort();
		held
	}

	fn due_before(registry: &mut TimerRegistry, armed: Vec<TimerDue>, flow: FlowId, millis: u64) -> Vec<TimerDue> {
		let mut found = registry.due_before(armed, flow, DateTime::from_millis(millis));
		found.sort();
		found
	}

	#[test]
	fn folding_an_arm_keeps_the_earliest_instant_for_an_operator() {
		// an entry must name the operator's earliest instant, otherwise the whole-flow skip hides a timer
		// already due
		let mut registry = TimerRegistry::default();

		due_before(&mut registry, vec![armed(1, 9_000), armed(1, 5_000)], flow(1), 0);
		due_before(&mut registry, vec![armed(2, 5_000), armed(2, 9_000)], flow(1), 0);

		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 5_000), armed(2, 5_000)]);
	}

	#[test]
	fn a_folded_arm_stays_armed_until_it_is_due() {
		// arms must fold on every call, otherwise a timer armed ahead of the watermark dies with its
		// transaction
		let mut registry = TimerRegistry::default();

		assert!(due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0).is_empty());

		assert_eq!(due_before(&mut registry, Vec::new(), flow(1), 5_000), vec![armed(1, 5_000)]);
	}

	#[test]
	fn an_entry_beyond_the_watermark_is_withheld_but_kept() {
		// withholding must never mean forgetting, otherwise the later round that could fire it never looks
		let mut registry = TimerRegistry::default();

		assert!(due_before(&mut registry, vec![armed(1, 9_000)], flow(1), 5_000).is_empty());

		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 9_000)]);
	}

	#[test]
	fn an_entry_exactly_at_the_watermark_is_due() {
		// the comparison must be inclusive, otherwise a timer the watermark has stopped exactly at never fires
		let mut registry = TimerRegistry::default();

		assert_eq!(due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 5_000), vec![armed(1, 5_000)]);
	}

	#[test]
	fn an_operator_past_the_watermark_is_withheld_while_a_sibling_fires() {
		// an open flow gate must never promote every operator in it, otherwise the fan-out scan returns
		// silently
		let mut registry = TimerRegistry::default();

		let found = due_before(&mut registry, vec![armed(1, 5_000), armed(2, 9_000)], flow(1), 5_000);

		assert_eq!(found, vec![armed(1, 5_000)]);
		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 5_000), armed(2, 9_000)]);
	}

	#[test]
	fn one_flow_never_surfaces_another_flows_operators() {
		// candidates must be keyed by flow, otherwise an operator is scanned against a watermark that is not
		// its own
		let mut registry = TimerRegistry::default();

		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);
		due_before(&mut registry, vec![armed(2, 5_000)], flow(2), 0);

		assert_eq!(due_before(&mut registry, Vec::new(), flow(1), 5_000), vec![armed(1, 5_000)]);
		assert_eq!(due_before(&mut registry, Vec::new(), flow(2), 5_000), vec![armed(2, 5_000)]);
	}

	#[test]
	fn a_flow_that_has_never_armed_anything_yields_no_candidates() {
		// the empty path must not create an entry, otherwise every quiet flow accumulates one per tick
		let mut registry = TimerRegistry::default();

		assert!(due_before(&mut registry, Vec::new(), flow(1), 5_000).is_empty());
		assert_eq!(entries(&registry, flow(1)), Vec::new());
	}

	#[test]
	fn refresh_overwrites_an_entry_with_its_next_instant() {
		// refresh must overwrite, otherwise the fired instant survives and the operator is rescanned forever
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);

		registry.refresh(flow(1), operator(1), Some(armed(1, 9_000)));

		assert!(due_before(&mut registry, Vec::new(), flow(1), 5_000).is_empty());
		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 9_000)]);
	}

	#[test]
	fn refresh_with_no_next_timer_drops_the_entry() {
		// none is the one report that authorises removal, otherwise a timerless operator stays a candidate for
		// good
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);

		registry.refresh(flow(1), operator(1), None);

		assert_eq!(entries(&registry, flow(1)), Vec::new());
	}

	#[test]
	fn rebuild_replaces_a_flows_entries_rather_than_merging_them() {
		// the rebuild input is the whole truth for that flow, otherwise a stale operator is scanned holding
		// nothing
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);

		registry.rebuild(flow(1), vec![armed(2, 7_000)]);

		assert_eq!(entries(&registry, flow(1)), vec![armed(2, 7_000)]);
	}

	#[test]
	fn rebuilding_with_nothing_armed_clears_the_flow() {
		// the clearing must come from the remove, because the fold returns early on an empty input
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);

		registry.rebuild(flow(1), Vec::new());

		assert_eq!(entries(&registry, flow(1)), Vec::new());
	}

	#[test]
	fn removing_an_operator_leaves_its_siblings_armed() {
		// removal must reach exactly the named operator, otherwise sibling timers die with nothing to re-arm
		// them
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000), armed(2, 5_000)], flow(1), 0);

		registry.remove_operator(flow(1), operator(1));

		assert_eq!(entries(&registry, flow(1)), vec![armed(2, 5_000)]);
	}

	#[test]
	fn removing_a_flow_drops_every_entry_it_held() {
		// a retired flow's entries must go, otherwise they name operators the engine can no longer dispatch to
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000), armed(2, 5_000)], flow(1), 0);
		due_before(&mut registry, vec![armed(3, 5_000)], flow(2), 0);

		registry.remove_flow(flow(1));

		assert_eq!(entries(&registry, flow(1)), Vec::new());
		assert_eq!(entries(&registry, flow(2)), vec![armed(3, 5_000)], "retiring one flow must spare the rest");
	}

	#[test]
	fn a_stage_that_is_never_applied_leaves_the_registry_untouched() {
		// the wheel removes die with the dropped transaction, so an index that advanced anyway hides a timer
		// that is still armed
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);

		let mut stage = registry.stage(flow(1));
		assert_eq!(
			stage.due_before(Vec::new(), DateTime::from_millis(5_000)),
			vec![armed(1, 5_000)],
			"the stage must see what the registry held when it was taken"
		);
		stage.refresh(operator(1), None);

		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 5_000)]);
	}

	#[test]
	fn a_stage_folds_arms_only_into_its_own_copy() {
		// an arm is a transactional write too, so folding it straight in outlives the transaction that never
		// committed it
		let registry = TimerRegistry::default();

		let mut stage = registry.stage(flow(1));
		stage.due_before(vec![armed(1, 5_000)], DateTime::from_millis(0));

		assert_eq!(entries(&registry, flow(1)), Vec::new());
	}

	#[test]
	fn applying_a_stage_moves_every_mutation_it_holds_onto_the_registry() {
		// the stage is the only record of what the dispatch drained, so a lost apply rescans and refires every
		// one of them
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000), armed(2, 5_000)], flow(1), 0);

		let mut stage = registry.stage(flow(1));
		stage.refresh(operator(1), Some(armed(1, 9_000)));
		stage.refresh(operator(2), None);
		registry.apply(stage);

		assert_eq!(entries(&registry, flow(1)), vec![armed(1, 9_000)]);
	}

	#[test]
	fn applying_a_stage_never_reaches_another_flow() {
		// a stage is scoped to one flow, otherwise its apply erases entries a sibling flow is about to scan
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);
		due_before(&mut registry, vec![armed(2, 5_000)], flow(2), 0);

		let mut stage = registry.stage(flow(1));
		stage.refresh(operator(1), None);
		registry.apply(stage);

		assert_eq!(entries(&registry, flow(1)), Vec::new());
		assert_eq!(entries(&registry, flow(2)), vec![armed(2, 5_000)]);
	}

	#[test]
	fn clearing_the_registry_drops_every_flow() {
		// clear must reach every flow, otherwise entries are read against flows that are no longer registered
		let mut registry = TimerRegistry::default();
		due_before(&mut registry, vec![armed(1, 5_000)], flow(1), 0);
		due_before(&mut registry, vec![armed(2, 5_000)], flow(2), 0);

		registry.clear();

		assert_eq!(entries(&registry, flow(1)), Vec::new());
		assert_eq!(entries(&registry, flow(2)), Vec::new());
	}
}
