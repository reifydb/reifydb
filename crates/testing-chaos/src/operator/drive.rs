// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	corpus::{Corpus, mix},
	operator::{
		expectation::{Bound, Expectation},
		model::Model,
		scenario::Scenario,
		session::Session,
		subject::Subject,
		view::MaterializedView,
		workload::{Op, Workload},
	},
};

pub struct DriveOutcome {
	pub corpus: Corpus,
	pub view: MaterializedView,


	pub divergence: Option<String>,
}

impl DriveOutcome {
	#[track_caller]
	pub fn assert_clean(self) -> Self {
		match &self.divergence {
			Some(report) => panic!("{report}"),
			None => self,
		}
	}
}

pub fn drive<S, W, M>(seed: u64, scenario: Scenario, subject: &mut S, workload: &W, model: &mut M) -> DriveOutcome
where
	S: Subject,
	W: Workload,
	M: Model<W::Row>,
{
	let mut rng = StdRng::seed_from_u64(seed);
	let mut session = Session::new(subject);
	let mut live: Vec<W::Row> = Vec::new();
	let mut next_row = RowNumber(1);
	let mut watermark = 0u64;

	let mut arrival = 0u64;
	let mut trace: Vec<String> = Vec::new();

	let mut fingerprint = mix(0, seed);


	for step in 0..scenario.steps {
		let roll = rng.random_range(0..100);

		if roll < scenario.tick_pct {
			let drawn = rng.random_range(1..=scenario.coord_span_ms / 2);
			watermark = watermark.saturating_add(drawn).min(arrival);
			trace.push(format!("step {step}: seal at {watermark}"));
			fingerprint = mix(mix(fingerprint, 1), watermark);
			session.tick(watermark).expect("tick must succeed");
			model.advance_ledger(watermark);
		} else if scenario.mixed_batches {
			let target = scenario.batch.draw(&mut rng);
			fingerprint = mix(fingerprint, 8);
			let mut ops: Vec<Op<W::Row>> = Vec::new();
			for _ in 0..target {
				let pick = rng.random_range(0..100);
				if !live.is_empty() && pick < scenario.remove_pct {
					let idx = rng.random_range(0..live.len());
					let row = live.remove(idx);
					let lanes = workload.lanes(&row);
					fingerprint = mix(mix(mix(fingerprint, 2), lanes.number), lanes.coord);
					model.retract(&row);
					ops.push(Op::Remove(row));
				} else if !live.is_empty() && pick < scenario.remove_pct + scenario.update_pct {
					let idx = rng.random_range(0..live.len());
					let pre = live[idx].clone();
					let post = workload.revalue(&mut rng, &pre);
					live[idx] = post.clone();
					let lanes = workload.lanes(&post);
					arrival = arrival.max(lanes.coord);
					fingerprint = mix(mix(mix(fingerprint, 3), lanes.number), lanes.value);
					let split = Scenario::rolls(scenario.update_as_remove_insert)
						&& scenario.remove_pct > 0 && rng.random::<f64>()
						< scenario.update_as_remove_insert;
					if split {
						fingerprint = mix(fingerprint, 5);
						model.retract(&pre);
						model.admit(&post);
						ops.push(Op::Remove(pre));
						ops.push(Op::Insert(post));
					} else {
						model.update(&pre, &post);
						let duplicate = Scenario::rolls(scenario.duplicate_update_burst)
							&& rng.random::<f64>() < scenario.duplicate_update_burst;
						ops.push(Op::Update(pre, post.clone()));
						if duplicate {
							fingerprint = mix(fingerprint, 6);
							model.update(&post, &post);
							ops.push(Op::Update(post.clone(), post));
						}
					}
				} else {
					let room = match scenario.max_live {
						Some(cap) => cap.saturating_sub(live.len()),
						None => usize::MAX,
					};
					if room == 0 {
						continue;
					}
					let row = workload.sample(&mut rng, next_row);
					next_row = RowNumber(next_row.0 + 1);
					let lanes = workload.lanes(&row);
					arrival = arrival.max(lanes.coord);
					fingerprint = mix(
						mix(mix(mix(fingerprint, 4), lanes.number), lanes.group),
						lanes.value,
					);
					let colliding = workload.identity(&row).and_then(|id| {
						live.iter().position(|e| workload.identity(e).as_ref() == Some(&id))
					});
					match colliding {
						Some(slot) => {
							let pre = live[slot].clone();
							fingerprint = mix(fingerprint, 7);
							model.update(&pre, &row);
							live[slot] = row.clone();
							ops.push(Op::Update(pre, row));
						}
						None => {
							if model.admit(&row) {
								live.push(row.clone());
							}
							ops.push(Op::Insert(row));
						}
					}
				}
			}
			trace.push(format!("step {step}: mixed batch of {} ops", ops.len()));
			if !ops.is_empty() {
				session.apply(workload.change(&ops)).expect("apply must succeed");
			}
		} else if !live.is_empty() && roll < scenario.tick_pct + scenario.remove_pct {
			let idx = rng.random_range(0..live.len());
			let row = live.remove(idx);
			trace.push(format!("step {step}: remove {row:?}"));
			let lanes = workload.lanes(&row);
			fingerprint = mix(mix(mix(mix(fingerprint, 2), lanes.number), lanes.group), lanes.coord);
			model.retract(&row);
			session.apply(workload.remove(&row)).expect("apply must succeed");
		} else if !live.is_empty() && roll < scenario.tick_pct + scenario.remove_pct + scenario.update_pct {
			let idx = rng.random_range(0..live.len());
			let pre = live[idx].clone();
			let post = workload.revalue(&mut rng, &pre);
			live[idx] = post.clone();
			trace.push(format!("step {step}: update {pre:?} -> {post:?}"));
			let lanes = workload.lanes(&post);
			arrival = arrival.max(lanes.coord);
			fingerprint = mix(mix(mix(mix(fingerprint, 3), lanes.number), lanes.coord), lanes.value);
			let split = Scenario::rolls(scenario.update_as_remove_insert)
				&& scenario.remove_pct > 0 && rng.random::<f64>()
				< scenario.update_as_remove_insert;
			if split {
				fingerprint = mix(fingerprint, 5);
				model.retract(&pre);
				model.admit(&post);
				session.apply(workload.remove(&pre)).expect("apply must succeed");
				session.apply(workload.insert(slice::from_ref(&post))).expect("apply must succeed");
			} else {
				model.update(&pre, &post);
				session.apply(workload.update(&pre, &post)).expect("apply must succeed");

				let duplicate = Scenario::rolls(scenario.duplicate_update_burst)
					&& rng.random::<f64>() < scenario.duplicate_update_burst;
				if duplicate {
					fingerprint = mix(fingerprint, 6);
					model.update(&post, &post);
					session.apply(workload.update(&post, &post)).expect("apply must succeed");
				}
			}
		} else {
			let drawn = scenario.batch.draw(&mut rng) as usize;
			let room = match scenario.max_live {
				Some(cap) => cap.saturating_sub(live.len()),
				None => usize::MAX,
			};
			let count = drawn.min(room);
			let mut batch: Vec<W::Row> = Vec::new();
			for _ in 0..count {
				batch.push(workload.sample(&mut rng, next_row));
				next_row = RowNumber(next_row.0 + 1);
			}
			trace.push(format!("step {step}: insert {batch:?}"));
			fingerprint = mix(fingerprint, 4);
			for row in &batch {
				let lanes = workload.lanes(row);
				arrival = arrival.max(lanes.coord);
				fingerprint = mix(
					mix(mix(mix(fingerprint, lanes.number), lanes.group), lanes.coord),
					lanes.value,
				);
			}

			let mut fresh: Vec<W::Row> = Vec::new();
			for row in &batch {
				let colliding = workload.identity(row).and_then(|id| {
					live.iter().position(|e| workload.identity(e).as_ref() == Some(&id))
				});
				match colliding {
					Some(slot) => {
						let pre = live[slot].clone();
						fingerprint = mix(fingerprint, 7);
						model.update(&pre, row);
						live[slot] = row.clone();
						session.apply(workload.update(&pre, row)).expect("apply must succeed");
					}
					None => {
						if model.admit(row) {
							live.push(row.clone());
						}
						fresh.push(row.clone());
					}
				}
			}
			if !fresh.is_empty() {
				session.apply(workload.insert(&fresh)).expect("apply must succeed");
			}
		}

		model.step_complete();

		if !session.incoherent().is_empty() {
			dump(&trace);
			let report = format!(
				"step {step}: the operator published an unfoldable diff stream: {:?}",
				session.incoherent()
			);
			return stopped(fingerprint, &trace, session, report);
		}

		if let Err(report) =
			model.live().check(session.view(), workload.projection(), workload.tolerances(), Bound::AtLeast)
		{
			dump(&trace);
			return stopped(fingerprint, &trace, session, format!("step {step}: {report}"));
		}
		if let Err(report) =
			model.all().check(session.view(), workload.projection(), workload.tolerances(), Bound::AtMost)
		{
			dump(&trace);
			return stopped(fingerprint, &trace, session, format!("step {step}: {report}"));
		}
	}

	let drain_at_ms = scenario.drain_at_ms.max(model.drain_floor());
	model.advance_ledger(drain_at_ms);

	let ticks = session.drain(drain_at_ms, 256).expect("drain tick must succeed");

	if let Err(report) =
		model.after_drain().check(session.view(), workload.projection(), workload.tolerances(), Bound::Exactly)
	{
		dump(&trace);
		let report = format!(
			"repeated ticks past every horizon must leave exactly what the model says survives, but \
			 after {ticks} ticks: {report}"
		);
		return stopped(fingerprint, &trace, session, report);
	}
	if !session.incoherent().is_empty() {
		dump(&trace);
		let report = format!("the drain published an unfoldable diff stream: {:?}", session.incoherent());
		return stopped(fingerprint, &trace, session, report);
	}

	DriveOutcome {
		corpus: Corpus::new(fingerprint, trace.len()),
		view: session.into_view(),
		divergence: None,
	}
}

fn stopped<S: Subject>(fingerprint: u64, trace: &[String], session: Session<'_, S>, report: String) -> DriveOutcome {
	DriveOutcome {
		corpus: Corpus::new(fingerprint, trace.len()),
		view: session.into_view(),
		divergence: Some(report),
	}
}

fn dump(trace: &[String]) {
	eprintln!("--- executed sequence ({} steps) ---", trace.len());
	for line in trace {
		eprintln!("{line}");
	}
	eprintln!("--- end sequence ---");
}
