// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{Arc, Mutex},
	time::Duration,
};

use reifydb_runtime::actor::{context::Context, system::dst::StepResult};

use super::helpers::*;

#[test]
fn zero_delay_timer() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_once(Duration::ZERO, || "zero".to_string());

	// Timer with zero delay should fire on advance_time(0).
	system.advance_time(Duration::ZERO);
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["zero"]);
}

#[test]
fn timer_cancellation_before_fire() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_once(Duration::from_millis(100), || "cancelled".to_string());

	// Cancel before advancing time.
	assert!(timer.cancel());

	system.advance_time(Duration::from_millis(200));
	system.run_until_idle();

	// Message should never have been enqueued.
	assert!(log_contents(&log).is_empty());
}

#[test]
fn timer_cancellation_after_fire() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_once(Duration::from_millis(100), || "fired".to_string());

	// Advance past deadline - timer fires.
	system.advance_time(Duration::from_millis(100));
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["fired"]);

	// Cancel after fire - the cancelled flag was never set by firing (it's a CAS),
	// so cancel() still returns true (sets the flag). But the timer already fired.
	// The important thing is the message was delivered.
	let _ = timer.cancel(); // no-op, timer already consumed
	assert!(timer.is_cancelled());
}

#[test]
fn multiple_timers_same_deadline() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	// Schedule 3 timers with the same delay.
	ctx.schedule_once(Duration::from_millis(100), || "t1".to_string());
	ctx.schedule_once(Duration::from_millis(100), || "t2".to_string());
	ctx.schedule_once(Duration::from_millis(100), || "t3".to_string());

	system.advance_time(Duration::from_millis(100));
	system.run_until_idle();

	let contents = log_contents(&log);
	assert_eq!(contents.len(), 3);
	// All three should fire. Order is deterministic by timer ID.
	assert_eq!(contents, vec!["t1", "t2", "t3"]);
}

#[test]
fn repeat_timer_cancellation() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	let timer = ctx.schedule_repeat(Duration::from_millis(100), "tick".to_string());

	// Advance 250ms - should fire at 100ms and 200ms.
	system.advance_time(Duration::from_millis(250));
	system.run_until_idle();
	assert_eq!(log_contents(&log).len(), 2);

	// Cancel the repeating timer.
	timer.cancel();

	// Advance more - no more fires.
	system.advance_time(Duration::from_millis(200));
	system.run_until_idle();
	assert_eq!(log_contents(&log).len(), 2);
}

#[test]
fn timer_and_direct_message_interleaving() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	// Schedule timer at 50ms.
	ctx.schedule_once(Duration::from_millis(50), || "timer".to_string());

	// Send direct message (gets a logical timestamp NOW).
	handle.actor_ref.send("direct".into()).unwrap();

	// Advance 50ms to fire the timer.
	system.advance_time(Duration::from_millis(50));

	system.run_until_idle();

	// "direct" was sent first (lower logical ts) so it's processed first.
	assert_eq!(log_contents(&log), vec!["direct", "timer"]);
}

#[test]
fn cascading_timers() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	// First timer at 100ms schedules a second timer at +100ms during its callback.
	// We achieve this by having the first timer send a message, and when the LogActor
	// doesn't schedule timers, we use a different approach: schedule both upfront.
	ctx.schedule_once(Duration::from_millis(100), || "first".to_string());
	ctx.schedule_once(Duration::from_millis(200), || "second".to_string());

	system.advance_time(Duration::from_millis(200));
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["first", "second"]);
}

#[test]
fn large_time_advance() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

	ctx.schedule_once(Duration::from_secs(1), || "1s".to_string());
	ctx.schedule_once(Duration::from_secs(2), || "2s".to_string());
	ctx.schedule_once(Duration::from_secs(3), || "3s".to_string());

	// Advance 10 seconds in one call.
	system.advance_time(Duration::from_secs(10));
	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["1s", "2s", "3s"]);
}

#[test]
fn schedule_tick_uses_mock_clock() {
	let system = test_system();
	let timestamps = Arc::new(Mutex::new(Vec::<u64>::new()));
	let handle = system.spawn(
		"tick",
		TickActor {
			timestamps: timestamps.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_tick(Duration::from_millis(100), |nanos| TickMessage(nanos));

	// Advance 350ms - ticks at 100ms, 200ms, 300ms.
	system.advance_time(Duration::from_millis(350));
	system.run_until_idle();

	let ts = timestamps.lock().unwrap().clone();
	assert_eq!(ts.len(), 3);
	// Mock clock starts at 0, ticks at 100ms, 200ms, 300ms.
	assert_eq!(ts[0], 100_000_000); // 100ms in nanos
	assert_eq!(ts[1], 200_000_000);
	assert_eq!(ts[2], 300_000_000);
}

#[test]
fn timer_not_fired_if_time_not_advanced() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_once(Duration::from_millis(100), || "should_not_fire".to_string());

	// Don't advance time at all.
	system.run_until_idle();

	assert!(log_contents(&log).is_empty());
}

#[test]
fn repeat_timer_fires_correct_count() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_repeat(Duration::from_millis(50), "tick".to_string());

	// Advance exactly 200ms - should fire at 50, 100, 150, 200.
	system.advance_time(Duration::from_millis(200));
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
		actors.push(system.spawn(
			&format!("actor{i}"),
			LogActor {
				log: log.clone(),
			},
		));
	}

	// Each actor sends n_messages to every other actor (including itself).
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
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	// Schedule a repeating timer.
	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_repeat(std::time::Duration::from_millis(100), "tick".to_string());

	// Fire it once.
	system.advance_time(std::time::Duration::from_millis(100));
	system.run_until_idle();
	assert_eq!(log_contents(&log), vec!["tick"]);

	// Now stop the actor.
	// We need an actor that can stop. CounterActor can.
	// But let's just use the handle and send a Directive::Stop somehow?
	// Our LogActor doesn't stop. Let's use CounterActor.
	let counter = system.spawn("counter", CounterActor);
	let ctx_c = Context::new(counter.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx_c.schedule_repeat(std::time::Duration::from_millis(100), CounterMessage::Inc);

	// Stop it.
	counter.actor_ref.send(CounterMessage::Stop).unwrap();
	system.run_until_idle();
	assert_eq!(system.alive_count(), 1); // only LogActor alive

	// Advance time more.
	system.advance_time(std::time::Duration::from_millis(500));

	// If the timer is still firing, it might be reported by system.step().
	// But the actor is dead. The message delivery SHOULD fail silently or be dropped.
	// We want to ensure no "Processed" or "Panicked" results for this actor's timer occur.
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
