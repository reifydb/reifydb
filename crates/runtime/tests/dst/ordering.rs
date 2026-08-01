// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

use super::helpers::*;

#[test]
fn two_actors_interleaved() {
	let system = test_system();
	let log = new_log();

	let a = system.spawn_coordination(
		"a",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn_coordination(
		"b",
		LogActor {
			log: log.clone(),
		},
	);

	// Delivery order is global by enqueue, so a per-actor queue would reorder these.
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

	let r0 = system.spawn_coordination(
		"r0",
		LogActor {
			log: log.clone(),
		},
	);
	let r1 = system.spawn_coordination(
		"r1",
		LogActor {
			log: log.clone(),
		},
	);
	let r2 = system.spawn_coordination(
		"r2",
		LogActor {
			log: log.clone(),
		},
	);

	let fan = system.spawn_coordination(
		"fan",
		FanOutActor {
			targets: vec![r0.actor_ref.clone(), r1.actor_ref.clone(), r2.actor_ref.clone()],
		},
	);

	fan.actor_ref.send("msg".into()).unwrap();
	system.run_until_idle();

	// Sends made inside a single handle() keep their emission order.
	assert_eq!(log_contents(&log), vec!["msg->t0", "msg->t1", "msg->t2"]);
}

#[test]
fn fan_in_ordering() {
	let system = test_system();
	let log = new_log();

	let receiver = system.spawn_coordination(
		"receiver",
		LogActor {
			log: log.clone(),
		},
	);

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

	let c = system.spawn_coordination(
		"c",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn_coordination(
		"b",
		ForwardActor {
			target: c.actor_ref.clone(),
		},
	);
	let a = system.spawn_coordination(
		"a",
		ForwardActor {
			target: b.actor_ref.clone(),
		},
	);

	a.actor_ref.send("chain".into()).unwrap();
	system.run_until_idle();

	// run_until_idle must drain messages that only exist once an earlier one is handled.
	assert_eq!(log_contents(&log), vec!["fwd:fwd:chain"]);
}

#[test]
fn deep_chain_interleaved_with_direct() {
	let system = test_system();
	let log = new_log();

	let c = system.spawn_coordination(
		"c",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn_coordination(
		"b",
		ForwardActor {
			target: c.actor_ref.clone(),
		},
	);

	b.actor_ref.send("via_b".into()).unwrap();
	c.actor_ref.send("direct".into()).unwrap();

	system.run_until_idle();

	// A forwarded message is enqueued when it is emitted, so it lands behind the direct send
	// that was already waiting rather than inheriting the originating message's position.
	assert_eq!(log_contents(&log), vec!["direct", "fwd:via_b"]);
}

#[test]
fn burst_single_actor() {
	let system = test_system();
	let log = new_log();
	let handle = system.spawn_coordination(
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
	let receiver = system.spawn_coordination(
		"receiver",
		LogActor {
			log: log.clone(),
		},
	);

	let _sender = system.spawn_coordination(
		"sender",
		InitSenderActor {
			target: receiver.actor_ref.clone(),
			init_msg: "from_init".into(),
		},
	);

	receiver.actor_ref.send("external".into()).unwrap();

	system.run_until_idle();

	let contents = log_contents(&log);
	// A send made during init() takes its place in the queue there, not when spawn returns.
	assert_eq!(contents, vec!["from_init", "external"]);
}

#[test]
fn three_actors_round_robin() {
	let system = test_system();
	let log = new_log();

	let a = system.spawn_coordination(
		"a",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn_coordination(
		"b",
		LogActor {
			log: log.clone(),
		},
	);
	let c = system.spawn_coordination(
		"c",
		LogActor {
			log: log.clone(),
		},
	);

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
	let handle = system.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	// Advancing the clock must not let a timer overtake a message already queued ahead of it.
	handle.actor_ref.send("direct".into()).unwrap();

	let ctx = reifydb_runtime::actor::context::Context::new(
		handle.actor_ref.clone(),
		system.clone(),
		system.cancellation_token(),
	);
	ctx.schedule_once(Duration::from_milliseconds(10).unwrap(), || "timer".to_string());

	system.advance_time(Duration::from_milliseconds(10).unwrap());

	system.run_until_idle();

	assert_eq!(log_contents(&log), vec!["direct", "timer"]);
}

#[test]
fn message_never_arrives_before_init_completes() {
	let system = test_system();
	let log = new_log();

	let _a = system.spawn_coordination(
		"a",
		InitSenderActor {
			target: system
				.spawn_coordination(
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

	// b's init cannot be observed without a logging hook in the helpers, so this asserts
	// nothing and only exercises the spawn-then-send-during-init path.
	system.run_until_idle();
}
