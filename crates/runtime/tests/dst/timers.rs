// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::{
	actor::{context::Context, system::dst::StepResult},
	sync::mutex::Mutex,
};
use reifydb_value::value::duration::Duration;

use super::helpers::*;

#[test]
fn zero_delay_timer() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_once(Duration::zero(), || "zero".to_string());

	// A zero delay must still fire, not be dropped as an already-past deadline.
	system.advance_time(Duration::zero());
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["zero"]);
}

#[test]
fn timer_cancellation_before_fire() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "cancelled".to_string());

	assert!(timer.cancel());

	system.advance_time(Duration::from_milliseconds(200).unwrap());
	system.run_until_idle();

	// Cancelling must drop the timer, not merely suppress the delivery.
	assert!(log_contents(&log).is_empty());
}

#[test]
fn timer_cancellation_after_fire() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "fired".to_string());

	system.advance_time(Duration::from_milliseconds(100).unwrap());
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["fired"]);

	// Firing never sets the cancelled flag, so a later cancel still flips it; the handle says
	// cancelled even though the message was already delivered.
	let _ = timer.cancel();
	assert!(timer.is_cancelled());
}

#[test]
fn multiple_timers_same_deadline() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "t1".to_string());
	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "t2".to_string());
	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "t3".to_string());

	system.advance_time(Duration::from_milliseconds(100).unwrap());
	system.run_until_idle();

	let contents = log_contents(&log);
	assert_eq!(contents.len(), 3);
	// A tied deadline must break by schedule order, or a replay is not reproducible.
	assert_eq!(contents, vec!["t1", "t2", "t3"]);
}

#[test]
fn repeat_timer_cancellation() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_repeat(Duration::from_milliseconds(100).unwrap(), "tick".to_string());

	system.advance_time(Duration::from_milliseconds(250).unwrap());
	system.run_until_idle();
	assert_eq!(log_contents(&log).len(), 2);

	// Cancelling must stop the rearm, not just the pending fire.
	timer.cancel();

	system.advance_time(Duration::from_milliseconds(200).unwrap());
	system.run_until_idle();
	assert_eq!(log_contents(&log).len(), 2);
}

#[test]
fn timer_and_direct_message_interleaving() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	ctx.schedule_once(Duration::from_milliseconds(50).unwrap(), || "timer".to_string());

	handle.actor_ref.send("direct".into()).unwrap();

	system.advance_time(Duration::from_milliseconds(50).unwrap());

	system.run_until_idle();

	// A fired timer joins the queue at its fire point, behind anything already waiting.
	assert_eq!(log_contents(&log), vec!["direct", "timer"]);
}

#[test]
fn cascading_timers() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	// LogActor cannot schedule from its handler, so both timers are scheduled upfront: this
	// covers staggered deadlines under one advance, not a genuine cascade.
	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "first".to_string());
	ctx.schedule_once(Duration::from_milliseconds(200).unwrap(), || "second".to_string());

	system.advance_time(Duration::from_milliseconds(200).unwrap());
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["first", "second"]);
}

#[test]
fn large_time_advance() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	ctx.schedule_once(Duration::from_seconds(1).unwrap(), || "1s".to_string());
	ctx.schedule_once(Duration::from_seconds(2).unwrap(), || "2s".to_string());
	ctx.schedule_once(Duration::from_seconds(3).unwrap(), || "3s".to_string());

	// One jump past every deadline must still yield each timer, in deadline order.
	system.advance_time(Duration::from_seconds(10).unwrap());
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["1s", "2s", "3s"]);
}

#[test]
fn schedule_tick_uses_mock_clock() {
	let system = test_system();
	let timestamps = Arc::new(Mutex::new(Vec::<u64>::new()));
	let handle = system.spawn_coordination(
		"tick",
		TickActor {
			timestamps: timestamps.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_tick(Duration::from_milliseconds(100).unwrap(), |nanos| TickMessage(nanos));

	system.advance_time(Duration::from_milliseconds(350).unwrap());
	system.run_until_idle();

	let ts = timestamps.lock().clone();
	assert_eq!(ts.len(), 3);
	// A tick carries its own deadline, not the wall clock, so a replay reproduces the values.
	assert_eq!(ts[0], 100_000_000);
	assert_eq!(ts[1], 200_000_000);
	assert_eq!(ts[2], 300_000_000);
}

#[test]
fn timer_not_fired_if_time_not_advanced() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "should_not_fire".to_string());

	// Nothing may fire off wall time; only advance_time moves the simulated clock.
	system.run_until_idle();

	assert!(log_contents(&log).is_empty());
}

#[test]
fn repeat_timer_fires_correct_count() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_repeat(Duration::from_milliseconds(50).unwrap(), "tick".to_string());

	// Landing exactly on a deadline must count it, so 200ms yields four fires and not three.
	system.advance_time(Duration::from_milliseconds(200).unwrap());
	system.run_until_idle();

	assert_eq!(log_contents(&log).len(), 4);
}

#[test]
fn message_storm_stress() {
	let system = test_system();
	let log = new_log();

	let n_actors = 10;
	let n_messages = 20;

	let mut actors = Vec::new();
	for i in 0..n_actors {
		actors.push(system.spawn_coordination(
			&format!("actor{i}"),
			LogActor {
				log: log.clone(),
			},
		));
	}

	for i in 0..n_actors {
		for j in 0..n_actors {
			for k in 0..n_messages {
				let _ = actors[j].actor_ref.send(format!("from_{i}_to_{j}_msg{k}"));
			}
		}
	}

	system.run_until_idle();

	let contents = log_contents(&log);
	assert_eq!(contents.len(), n_actors * n_actors * n_messages);
}

#[test]
fn timers_must_be_cancelled_when_actor_stops() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_repeat(Duration::from_milliseconds(100).unwrap(), "tick".to_string());

	system.advance_time(Duration::from_milliseconds(100).unwrap());
	system.run_until_idle();
	assert_eq!(log_contents(&log), vec!["tick"]);

	// LogActor never stops, so the stoppable half of the check needs CounterActor.
	let counter = system.spawn_coordination("counter", CounterActor);
	let ctx_c = Context::new(counter.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx_c.schedule_repeat(Duration::from_milliseconds(100).unwrap(), CounterMessage::Inc);

	counter.actor_ref.send(CounterMessage::Stop).unwrap();
	system.run_until_idle();
	assert_eq!(system.alive_count(), 1);

	system.advance_time(Duration::from_milliseconds(500).unwrap());

	// A repeating timer that outlives its actor would rearm forever and never let the
	// simulation reach idle.
	loop {
		match system.step() {
			StepResult::Idle => break,
			StepResult::Processed {
				actor_id,
			} if actor_id == 1 => {
				panic!("Timer for dead actor was still processed!");
			}
			_ => {}
		}
	}
}
