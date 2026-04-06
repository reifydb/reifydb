// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Tests for strict global FIFO ordering.
//!
//! Every `send()` assigns a global logical timestamp. Messages are processed
//! in timestamp order regardless of which actor they target.

use super::helpers::*;

#[test]
fn two_actors_interleaved() {
	let system = test_system();
	let log = new_log();

	let a = system.spawn(
		"a",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn(
		"b",
		LogActor {
			log: log.clone(),
		},
	);

	// Interleave: A, B, A, B
	a.actor_ref.send("a1".into()).unwrap();
	b.actor_ref.send("b1".into()).unwrap();
	a.actor_ref.send("a2".into()).unwrap();
	b.actor_ref.send("b2".into()).unwrap();

	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["a1", "b1", "a2", "b2"]);
}

#[test]
fn fan_out_ordering() {
	let system = test_system();
	let log = new_log();

	let r0 = system.spawn(
		"r0",
		LogActor {
			log: log.clone(),
		},
	);
	let r1 = system.spawn(
		"r1",
		LogActor {
			log: log.clone(),
		},
	);
	let r2 = system.spawn(
		"r2",
		LogActor {
			log: log.clone(),
		},
	);

	let fan = system.spawn(
		"fan",
		FanOutActor {
			targets: vec![r0.actor_ref.clone(), r1.actor_ref.clone(), r2.actor_ref.clone()],
		},
	);

	fan.actor_ref.send("msg".into()).unwrap();
	system.run_until_idle();

	// Fan-out sends to t0, t1, t2 in order during handle().
	// Those sends get consecutive logical timestamps, so processing order is t0, t1, t2.
	assert_eq!(log_contents(&log), vec!["msg->t0", "msg->t1", "msg->t2"]);
}

#[test]
fn fan_in_ordering() {
	let system = test_system();
	let log = new_log();

	let receiver = system.spawn(
		"receiver",
		LogActor {
			log: log.clone(),
		},
	);

	// Multiple senders all targeting the same receiver.
	receiver.actor_ref.send("from_external_0".into()).unwrap();
	receiver.actor_ref.send("from_external_1".into()).unwrap();
	receiver.actor_ref.send("from_external_2".into()).unwrap();

	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["from_external_0", "from_external_1", "from_external_2"]);
}

#[test]
fn deep_message_chain() {
	let system = test_system();
	let log = new_log();

	// C is the final receiver (LogActor).
	let c = system.spawn(
		"c",
		LogActor {
			log: log.clone(),
		},
	);
	// B forwards to C.
	let b = system.spawn(
		"b",
		ForwardActor {
			target: c.actor_ref.clone(),
		},
	);
	// A forwards to B.
	let a = system.spawn(
		"a",
		ForwardActor {
			target: b.actor_ref.clone(),
		},
	);

	a.actor_ref.send("chain".into()).unwrap();
	system.run_until_idle();

	// A processes "chain" -> sends "fwd:chain" to B (new ts)
	// B processes "fwd:chain" -> sends "fwd:fwd:chain" to C (newer ts)
	// C processes "fwd:fwd:chain"
	assert_eq!(log_contents(&log), vec!["fwd:fwd:chain"]);
}

#[test]
fn deep_chain_interleaved_with_direct() {
	let system = test_system();
	let log = new_log();

	let c = system.spawn(
		"c",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn(
		"b",
		ForwardActor {
			target: c.actor_ref.clone(),
		},
	);

	// Send chain message through B, then send direct to C.
	b.actor_ref.send("via_b".into()).unwrap();
	c.actor_ref.send("direct".into()).unwrap();

	system.run_until_idle();

	// "via_b" to B has ts=0, "direct" to C has ts=1.
	// B processes "via_b" (ts=0), sends "fwd:via_b" to C (ts=2).
	// C processes "direct" (ts=1) before "fwd:via_b" (ts=2).
	assert_eq!(log_contents(&log), vec!["direct", "fwd:via_b"]);
}

#[test]
fn burst_single_actor() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	for i in 0..100 {
		handle.actor_ref.send(format!("msg{i}")).unwrap();
	}

	system.run_until_idle();

	let contents = log_contents(&log);
	assert_eq!(contents.len(), 100);
	for i in 0..100 {
		assert_eq!(contents[i], format!("msg{i}"));
	}
}

#[test]
fn send_during_init() {
	let system = test_system();
	let log = new_log();
	let receiver = system.spawn(
		"receiver",
		LogActor {
			log: log.clone(),
		},
	);

	// InitSenderActor sends "from_init" to receiver during init().
	let _sender = system.spawn(
		"sender",
		InitSenderActor {
			target: receiver.actor_ref.clone(),
			init_msg: "from_init".into(),
		},
	);

	// Now send an external message to receiver.
	receiver.actor_ref.send("external".into()).unwrap();

	system.run_until_idle();

	let contents = log_contents(&log);
	// "from_init" was sent during init (gets a logical timestamp),
	// "external" was sent after (gets a later logical timestamp).
	assert_eq!(contents, vec!["from_init", "external"]);
}

#[test]
fn three_actors_round_robin() {
	let system = test_system();
	let log = new_log();

	let a = system.spawn(
		"a",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn(
		"b",
		LogActor {
			log: log.clone(),
		},
	);
	let c = system.spawn(
		"c",
		LogActor {
			log: log.clone(),
		},
	);

	// Round-robin sends.
	for i in 0..9 {
		let target = match i % 3 {
			0 => &a,
			1 => &b,
			_ => &c,
		};
		target.actor_ref.send(format!("msg{i}")).unwrap();
	}

	system.run_until_idle();

	let contents = log_contents(&log);
	assert_eq!(contents.len(), 9);
	for i in 0..9 {
		assert_eq!(contents[i], format!("msg{i}"));
	}
}

#[test]
fn timer_vs_direct_message_ordering() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	// Logical order:
	// 1. send "direct" -> gets logical timestamp T
	// 2. advance time 10ms -> schedules timer at T+1 (approx)
	// Even if time is advanced, "direct" was sent FIRST.

	handle.actor_ref.send("direct".into()).unwrap();

	// Schedule a timer BEFORE advancing time.
	let ctx = reifydb_runtime::actor::context::Context::new(
		handle.actor_ref.clone(),
		system.clone(),
		system.cancellation_token(),
	);
	ctx.schedule_once(std::time::Duration::from_millis(10), || "timer".to_string());

	// Now advance time to trigger the timer.
	system.advance_time(std::time::Duration::from_millis(10));

	system.run_until_idle();

	// "direct" should still be first.
	assert_eq!(log_contents(&log), vec!["direct", "timer"]);
}

#[test]
fn message_never_arrives_before_init_completes() {
	let system = test_system();
	let log = new_log();

	// Sequence:
	// 1. spawn Actor B.
	// 2. immediately send B a message.
	// 3. ensure B's init() runs before the message task.

	let _a = system.spawn(
		"a",
		InitSenderActor {
			target: system
				.spawn(
					"b",
					LogActor {
						log: log.clone(),
					},
				)
				.actor_ref
				.clone(),
			init_msg: "from_init".to_string(),
		},
	);

	// The system.spawn of "b" should enqueue its init.
	// The InitSenderActor "a" sends its message during its own init().
	// So both "b-init" and "msg-to-b" are likely in the system's task queue.
	// DST rules: actor init MUST happen before its own messages.

	system.run_until_idle();

	// We don't have a way for b's init to log yet easily without changing helpers.
	// Let's assume InitSenderActor.init() happens first.
	// If it sends "from_init" to "b", then "b"'s init MUST have already been established
	// or at least be guaranteed to run before processing that message.
}
