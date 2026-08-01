// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::{actor::system::dst::StepResult, sync::mutex::Mutex};

use super::helpers::*;

#[test]
fn immediate_stop_on_first_message() {
	let system = test_system();
	let handle = system.spawn_coordination("counter", CounterActor);

	handle.actor_ref.send(CounterMessage::Stop).unwrap();

	assert!(matches!(
		system.step(),
		StepResult::Stopped {
			actor_id: 0
		}
	));

	assert_eq!(system.alive_count(), 0);
	assert!(handle.actor_ref.send(CounterMessage::Inc).is_err());
}

#[test]
fn post_stop_called_on_directive_stop() {
	let system = test_system();
	let stopped = Arc::new(Mutex::new(false));
	let handle = system.spawn_coordination(
		"ps",
		PostStopActor {
			stopped: stopped.clone(),
		},
	);

	handle.actor_ref.send(PostStopMessage::Stop).unwrap();
	system.run_until_idle();

	assert!(*stopped.lock());
}

#[test]
fn post_stop_called_on_panic() {
	let system = test_system();
	let stopped = Arc::new(Mutex::new(false));
	let handle = system.spawn_coordination(
		"ps",
		PostStopActor {
			stopped: stopped.clone(),
		},
	);

	handle.actor_ref.send(PostStopMessage::Boom).unwrap();
	system.run_until_idle();

	assert!(*stopped.lock());
}

#[test]
fn post_stop_called_on_shutdown() {
	let system = test_system();
	let stopped = Arc::new(Mutex::new(false));
	let _handle = system.spawn_coordination(
		"ps",
		PostStopActor {
			stopped: stopped.clone(),
		},
	);

	system.shutdown();

	assert!(*stopped.lock());
}

#[test]
fn send_to_dead_actor_returns_closed() {
	let system = test_system();
	let handle = system.spawn_coordination("counter", CounterActor);

	handle.actor_ref.send(CounterMessage::Stop).unwrap();
	system.run_until_idle();

	let result = handle.actor_ref.send(CounterMessage::Inc);
	assert!(result.is_err());
}

#[test]
fn panic_does_not_kill_system() {
	let system = test_system();
	let log = new_log();

	let panicker = system.spawn_coordination("panicker", PanicActor);
	let logger = system.spawn_coordination(
		"logger",
		LogActor {
			log: log.clone(),
		},
	);

	panicker.actor_ref.send(PanicMessage::Boom).unwrap();
	logger.actor_ref.send("still_alive".into()).unwrap();

	system.run_until_idle();

	// One actor's panic must not take the system or its siblings down with it.
	assert_eq!(log_contents(&log), vec!["still_alive"]);
	assert_eq!(system.alive_count(), 1);
}

#[test]
fn panic_payload_is_captured() {
	let system = test_system();
	let handle = system.spawn_coordination("panicker", PanicActor);

	handle.actor_ref.send(PanicMessage::Boom).unwrap();

	match system.step() {
		StepResult::Panicked {
			actor_id: 0,
			payload,
		} => {
			let msg = payload.downcast_ref::<&str>().unwrap();
			assert_eq!(*msg, "actor boom");
		}
		_ => panic!("expected StepResult::Panicked"),
	}
}

#[test]
fn panic_during_init_must_be_captured() {
	let system = test_system();

	// init() runs inside spawn(), so a panic there has no step frame to surface on until the
	// first step; losing it would leave a silently dead actor.
	let _handle = system.spawn_coordination("init_panicker", InitPanicActor);

	match system.step() {
		StepResult::Panicked {
			actor_id: 0,
			payload,
		} => {
			let msg = payload.downcast_ref::<&str>().unwrap();
			assert_eq!(*msg, "init boom");
		}
		_ => panic!("Expected StepResult::Panicked when init panics"),
	}

	assert_eq!(system.alive_count(), 0);
}

#[test]
fn panic_during_post_stop_must_be_captured() {
	let system = test_system();
	let handle = system.spawn_coordination("post_stop_panicker", PostStopPanicActor);

	handle.actor_ref.send(()).unwrap();

	let res1 = system.step();
	assert!(matches!(
		res1,
		StepResult::Stopped {
			actor_id: 0
		}
	));

	// post_stop runs after the actor is already stopped, so its panic has no handler frame and
	// would be lost unless the system reports it as a step result.
	match system.step() {
		StepResult::Panicked {
			actor_id: 0,
			payload,
		} => {
			let msg = payload.downcast_ref::<&str>().unwrap();
			assert_eq!(*msg, "post_stop boom");
		}
		_ => panic!("Expected StepResult::Panicked when post_stop panics"),
	}
}

#[test]
fn shutdown_cancels_all_actors() {
	let system = test_system();

	let h1 = system.spawn_coordination("a", CounterActor);
	let h2 = system.spawn_coordination("b", CounterActor);
	let h3 = system.spawn_coordination("c", CounterActor);

	assert_eq!(system.alive_count(), 3);

	system.shutdown();

	assert_eq!(system.alive_count(), 0);
	assert!(h1.actor_ref.send(CounterMessage::Inc).is_err());
	assert!(h2.actor_ref.send(CounterMessage::Inc).is_err());
	assert!(h3.actor_ref.send(CounterMessage::Inc).is_err());
}

#[test]
fn cancellation_token_propagated_on_shutdown() {
	let system = test_system();
	let cancel = system.cancellation_token();

	let _h = system.spawn_coordination("a", CounterActor);

	assert!(!cancel.is_cancelled());

	system.shutdown();

	assert!(cancel.is_cancelled());
}

#[test]
fn spawn_during_handling() {
	let system = test_system();
	let log = new_log();

	let handle = system.spawn_coordination(
		"parent",
		SpawnChildActor {
			log: log.clone(),
		},
	);

	handle.actor_ref.send(SpawnChildMessage::SpawnAndSend("child_msg".into())).unwrap();

	system.run_until_idle();

	// An actor spawned from inside a handler must join the same step loop, not a detached one.
	assert_eq!(log_contents(&log), vec!["child_msg"]);
}

#[test]
fn spawn_during_shutdown_must_fail() {
	let system = test_system();

	system.shutdown();
	assert!(system.is_cancelled());

	// A late spawn must hand back a dead ref rather than a live actor nothing will ever step.
	let handle = system.spawn_coordination("late_comer", CounterActor);

	assert!(
		handle.actor_ref.send(CounterMessage::Inc).is_err(),
		"Should not be able to send to actor spawned after shutdown"
	);

	match system.step() {
		StepResult::Idle => {}
		StepResult::Processed {
			actor_id,
		} if actor_id == 1 => {
			panic!("Actor spawned after shutdown was processed!");
		}
		_ => {}
	}
}
