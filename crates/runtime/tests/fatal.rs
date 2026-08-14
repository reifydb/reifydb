// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(not(target_arch = "wasm32"))]

use std::{
	env,
	os::unix::process::ExitStatusExt,
	process::{Command, Output},
	thread,
};

use reifydb_runtime::fatal::{FatalConfig, install, report::ISSUE_URL};

const TRIGGER: &str = "REIFYDB_FATAL_TEST_TRIGGER";
const SIGABRT: i32 = 6;

fn triggered() -> Option<String> {
	env::var(TRIGGER).ok()
}

fn run_child(test_name: &str, trigger: &str) -> Output {
	Command::new(env::current_exe().expect("the test binary must be locatable to re-enter it"))
		.args(["--exact", test_name, "--nocapture"])
		.env(TRIGGER, trigger)
		.env("RUST_BACKTRACE", "1")
		.output()
		.expect("the child test process must start")
}

fn stderr_of(output: &Output) -> String {
	String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn an_armed_panic_aborts_the_process_and_prints_a_ticket_grade_report() {
	// A panic that only logs is the bug this whole mechanism exists to close, so an exit that is not SIGABRT means
	// the process survived something impossible.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::armed(true));
		panic!("deliberate fatal from the child");
	}

	let output = run_child("an_armed_panic_aborts_the_process_and_prints_a_ticket_grade_report", "panic");
	let stderr = stderr_of(&output);

	assert_eq!(
		output.status.signal(),
		Some(SIGABRT),
		"an armed fatal must abort, not exit; got status {:?} with stderr:\n{}",
		output.status,
		stderr
	);
	assert!(stderr.contains("REIFYDB FATAL"), "the report must be greppable; stderr was:\n{}", stderr);
	assert!(stderr.contains("deliberate fatal from the child"), "the panic message must survive into the report");
	assert!(stderr.contains("kind:      panic"));
	assert!(stderr.contains(ISSUE_URL), "a report nobody can file is not a report");
	assert!(stderr.contains("thread:"), "the report must name the thread the panic came from");
	assert!(stderr.contains("version:"), "a ticket without a version cannot be triaged");
}

#[test]
fn the_report_carries_the_stack_from_the_panic_site_not_the_handler() {
	// Capturing in a catch_unwind arm yields the landing pad instead, which is exactly the misleading backtrace the
	// hook exists to replace.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::armed(true));
		fn the_frame_that_must_appear() -> ! {
			panic!("deliberate fatal from a named frame");
		}
		the_frame_that_must_appear();
	}

	let output = run_child("the_report_carries_the_stack_from_the_panic_site_not_the_handler", "panic");
	let stderr = stderr_of(&output);

	assert_eq!(output.status.signal(), Some(SIGABRT), "stderr was:\n{}", stderr);
	assert!(stderr.contains("backtrace:"), "stderr was:\n{}", stderr);
	assert!(
		stderr.contains("the_frame_that_must_appear"),
		"the panicking frame must be in the stack, otherwise the backtrace is the handler's and useless; stderr was:\n{}",
		stderr
	);
}

#[test]
fn a_panic_on_a_background_thread_takes_the_whole_process_down() {
	// This is the swallow the runtime pools perform today: the actor dies, the process lives, and the database
	// keeps serving from a wedged store.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::armed(true));
		let worker = thread::Builder::new()
			.name("fatal-test-worker".to_string())
			.spawn(|| panic!("deliberate fatal from a background thread"))
			.expect("the worker thread must spawn");
		let _ = worker.join();
		println!("PARENT SURVIVED THE WORKER PANIC");
	}

	let output = run_child("a_panic_on_a_background_thread_takes_the_whole_process_down", "panic");
	let stderr = stderr_of(&output);
	let stdout = String::from_utf8_lossy(&output.stdout).to_string();

	assert_eq!(
		output.status.signal(),
		Some(SIGABRT),
		"a background panic must kill the process; stdout:\n{}\nstderr:\n{}",
		stdout,
		stderr
	);
	assert!(
		!stdout.contains("PARENT SURVIVED THE WORKER PANIC"),
		"the main thread must never resume past a background fatal"
	);
	assert!(
		stderr.contains("fatal-test-worker"),
		"the report must name the thread that died, or an operator cannot tell which actor it was; stderr was:\n{}",
		stderr
	);
}

#[test]
fn an_armed_fatal_beats_a_catch_unwind_that_would_have_swallowed_it() {
	// The runtime pools wrap every actor batch in exactly this shape and merely log, so the hook must win the race
	// or the swallow survives the fix.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::armed(true));
		let swallowed = std::panic::catch_unwind(|| panic!("deliberate fatal inside a catch_unwind"));
		println!("SWALLOWED: {}", swallowed.is_err());
	}

	let output = run_child("an_armed_fatal_beats_a_catch_unwind_that_would_have_swallowed_it", "panic");
	let stdout = String::from_utf8_lossy(&output.stdout).to_string();
	let stderr = stderr_of(&output);

	assert_eq!(
		output.status.signal(),
		Some(SIGABRT),
		"a catch_unwind must not be able to rescue a panic once armed; stdout:\n{}\nstderr:\n{}",
		stdout,
		stderr
	);
	assert!(
		!stdout.contains("SWALLOWED"),
		"reaching the catch arm means the process outlived something impossible"
	);
}

#[test]
fn an_unexpected_error_can_be_routed_into_the_same_fatal_path() {
	// An Err with no handler is the same class of failure as a panic, and swallowing it keeps the system running on
	// wrong data.
	if triggered().as_deref() == Some("error") {
		install(FatalConfig::armed(true));
		let outcome: Result<(), String> = Err("storage flush could not reach sqlite".to_string());
		reifydb_runtime::fatal_on_err!(outcome, "operator flush");
		unreachable!("fatal_on_err must not return on the Err arm");
	}

	let output = run_child("an_unexpected_error_can_be_routed_into_the_same_fatal_path", "error");
	let stderr = stderr_of(&output);

	assert_eq!(output.status.signal(), Some(SIGABRT), "stderr was:\n{}", stderr);
	assert!(stderr.contains("kind:      unexpected error"), "stderr was:\n{}", stderr);
	assert!(stderr.contains("component: operator flush"), "the component must localise the failure");
	assert!(stderr.contains("storage flush could not reach sqlite"));
}

#[test]
fn a_violated_invariant_can_be_declared_fatal_without_a_panic() {
	// Some invariants are checked where there is nothing to return, and reporting them as a plain panic loses the
	// stated reason.
	if triggered().as_deref() == Some("invariant") {
		install(FatalConfig::armed(true));
		reifydb_runtime::fatal!("watermark moved backwards: {} -> {}", 9, 4);
	}

	let output = run_child("a_violated_invariant_can_be_declared_fatal_without_a_panic", "invariant");
	let stderr = stderr_of(&output);

	assert_eq!(output.status.signal(), Some(SIGABRT), "stderr was:\n{}", stderr);
	assert!(stderr.contains("kind:      invariant violated"), "stderr was:\n{}", stderr);
	assert!(stderr.contains("watermark moved backwards: 9 -> 4"));
	assert!(stderr.contains("location:  crates/runtime/tests/fatal.rs"), "the macro must record its own call site");
}

#[test]
fn a_disarmed_hook_reports_but_lets_the_panic_unwind() {
	// Disarming exists so tests can panic on purpose, and a disarmed hook that still aborted would take the suite
	// with it.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::disarmed());
		panic!("deliberate fatal while disarmed");
	}

	let output = run_child("a_disarmed_hook_reports_but_lets_the_panic_unwind", "panic");
	let stderr = stderr_of(&output);

	assert_eq!(output.status.signal(), None, "a disarmed hook must not abort; stderr was:\n{}", stderr);
	assert!(
		stderr.contains("REIFYDB FATAL"),
		"disarming silences the abort, never the report; stderr was:\n{}",
		stderr
	);
	assert!(stderr.contains("deliberate fatal while disarmed"));
}

#[test]
fn the_env_var_disarms_a_config_that_asked_to_be_armed() {
	// The env override is the escape hatch when the caller cannot reach the builder, so it must beat the default
	// rather than merely supply it.
	if triggered().as_deref() == Some("panic") {
		install(FatalConfig::default());
		panic!("deliberate fatal with the env override set");
	}

	let output = Command::new(env::current_exe().expect("the test binary must be locatable"))
		.args(["--exact", "the_env_var_disarms_a_config_that_asked_to_be_armed", "--nocapture"])
		.env(TRIGGER, "panic")
		.env("REIFYDB_FATAL", "0")
		.output()
		.expect("the child test process must start");
	let stderr = stderr_of(&output);

	assert_eq!(output.status.signal(), None, "REIFYDB_FATAL=0 must disarm the default config; stderr:\n{}", stderr);
	assert!(stderr.contains("REIFYDB FATAL"), "the report still fires when disarmed");
}
