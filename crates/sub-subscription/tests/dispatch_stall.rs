// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(not(target_arch = "wasm32"))]

use std::{
	env,
	io::Read,
	os::unix::process::ExitStatusExt,
	process::{self, Command, ExitStatus, Stdio},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
		mpsc::{Receiver, Sender, channel},
	},
	thread::{self, JoinHandle, sleep},
	time::Instant,
};

use reifydb::{
	Params, embedded,
	routine::abi::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
	testing::db::TestDb,
};
use reifydb_core::{
	interface::{
		catalog::{
			config::ConfigKey,
			id::SubscriptionId,
			subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
		},
		cdc::CdcConsumerId,
	},
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId, value_type::ValueType};

const STALL_CHILD_ENV: &str = "REIFYDB_SUBSCRIPTION_STALL_CHILD";
const SUBSCRIPTION_MARKER: &str = "stall child subscription: ";
const INSERT_VERSION_MARKER: &str = "stall child insert version: ";
const WORKERS: u64 = 2;

struct Latch {
	closed: AtomicBool,
	entered: Sender<()>,
	release: Mutex<Receiver<()>>,
}

struct LatchedPass {
	info: RoutineInfo,
	latch: Arc<Latch>,
}

impl<'a> Routine<FunctionContext<'a>> for LatchedPass {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types[0].clone()
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if self.latch.closed.load(Ordering::Acquire) {
			self.latch.entered.send(()).unwrap();
			self.latch.release.lock().recv().unwrap();
		}
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), args[0].clone())]))
	}
}

impl Function for LatchedPass {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}

fn subscribe(db: &TestDb, rql: &str) -> SubscriptionId {
	let options = SubscribeOptions {
		hydration: HydrationConfig {
			enabled: false,
			max_rows: None,
		},
		..SubscribeOptions::default()
	};
	match db.engine().subscribe_as(IdentityId::root(), rql, Params::None, options).expect("subscribe as root") {
		SubscribeOutcome::Local {
			id,
		} => id,
		SubscribeOutcome::Remote {
			address,
			..
		} => panic!("expected a local subscription, got a remote one at {}", address),
	}
}

#[test]
fn a_worker_that_never_finishes_a_dispatch_aborts_naming_the_worker() {
	// A worker stuck inside a dispatch must abort by name once the wait timeout passes, or delivery stops silently.
	if env::var(STALL_CHILD_ENV).is_ok() {
		run_stall_child();
	}

	let child = run_child_with_limit(
		"a_worker_that_never_finishes_a_dispatch_aborts_naming_the_worker",
		Duration::from_seconds(60).unwrap(),
	);
	assert!(
		!child.timed_out,
		"the stalled child must abort before its wall-clock limit\nstderr:\n{}",
		child.stderr
	);
	assert_eq!(
		child.status.signal(),
		Some(6),
		"a stuck worker must abort with SIGABRT, but the child exited with {:?}\nstderr:\n{}",
		child.status,
		child.stderr
	);
	let subscription: u64 = announced(&child.stdout, SUBSCRIPTION_MARKER);
	let insert_version: u64 = announced(&child.stdout, INSERT_VERSION_MARKER);
	assert_eq!(
		report_field(&child.stderr, "consumer"),
		Some(format!("{:?}", CdcConsumerId::subscription_consumer())),
		"the abort must name the stalled consumer\nstderr:\n{}",
		child.stderr
	);
	assert_eq!(
		report_field(&child.stderr, "pending"),
		Some(format!("subscription-worker-{}", subscription % WORKERS)),
		"the abort must name exactly the worker that never finished\nstderr:\n{}",
		child.stderr
	);
	let batch: u64 = report_field(&child.stderr, "batch")
		.unwrap_or_else(|| panic!("the abort must name the stalled batch\nstderr:\n{}", child.stderr))
		.parse()
		.unwrap_or_else(|_| panic!("the stalled batch must be a commit version\nstderr:\n{}", child.stderr));
	assert!(
		batch >= insert_version,
		"the stalled batch {} must cover the held insert at version {}\nstderr:\n{}",
		batch,
		insert_version,
		child.stderr
	);
}

fn run_stall_child() -> ! {
	let (entered_tx, entered_rx) = channel();
	let (release_tx, release_rx) = channel();
	let latch = Arc::new(Latch {
		closed: AtomicBool::new(false),
		entered: entered_tx,
		release: Mutex::new(release_rx),
	});
	let function = Arc::new(LatchedPass {
		info: RoutineInfo::new("latch::pass"),
		latch: latch.clone(),
	});
	let db = TestDb::from(
		embedded::memory()
			.with_routines(move |routines| routines.register_function(function))
			.with_config(ConfigKey::ThreadsCoordination, Value::Uint2(2))
			.with_config(ConfigKey::SubscriptionWorkerThreads, Value::Uint2(WORKERS as u16))
			.with_config(
				ConfigKey::CdcConsumeWaitTimeout,
				Value::Duration(Duration::from_seconds(1).unwrap()),
			)
			.build()
			.unwrap(),
	);
	assert_eq!(
		db.engine().spawner().pools().coordination_thread_count(),
		2,
		"the poll actor needs a coordination thread the held worker does not occupy"
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, v: int4 }");

	let held = subscribe(&db, "from app::t filter { latch::pass(v) > 0 }");
	latch.closed.store(true, Ordering::Release);
	db.command("INSERT app::t [{id: 1, v: 10}]");
	let insert_version = db.watermarks().tx().current().expect("current version");
	entered_rx
		.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
		.expect("the held worker must reach the latch while evaluating the insert");
	println!("{SUBSCRIPTION_MARKER}{}", held.0);
	println!("{INSERT_VERSION_MARKER}{}", insert_version.0);

	sleep(Duration::from_seconds(20).unwrap().to_std());
	drop(release_tx);
	process::exit(0)
}

fn announced(stdout: &str, marker: &str) -> u64 {
	stdout.lines()
		.find_map(|line| line.split_once(marker).map(|(_, value)| value))
		.unwrap_or_else(|| panic!("the child must announce '{}'\nstdout:\n{}", marker, stdout))
		.parse()
		.unwrap_or_else(|_| panic!("the child announced a non-number for '{}'\nstdout:\n{}", marker, stdout))
}

struct ChildOutput {
	status: ExitStatus,
	timed_out: bool,
	stdout: String,
	stderr: String,
}

fn run_child_with_limit(test_name: &str, limit: Duration) -> ChildOutput {
	let exe = env::current_exe().expect("Failed to resolve test binary path");
	let mut child = Command::new(exe)
		.args([test_name, "--exact", "--nocapture", "--test-threads=1"])
		.env(STALL_CHILD_ENV, "1")
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("Failed to spawn child process");
	let stdout = read_in_background(child.stdout.take().expect("child stdout is piped"));
	let stderr = read_in_background(child.stderr.take().expect("child stderr is piped"));

	let deadline = Instant::now() + limit.to_std();
	let mut timed_out = false;
	let status = loop {
		if let Some(status) = child.try_wait().expect("Failed to poll child process") {
			break status;
		}
		if Instant::now() >= deadline {
			timed_out = true;
			child.kill().expect("Failed to kill child past its wall-clock limit");
			break child.wait().expect("Failed to reap killed child");
		}
		sleep(Duration::from_milliseconds(10).unwrap().to_std());
	};

	ChildOutput {
		status,
		timed_out,
		stdout: stdout.join().expect("child stdout reader panicked"),
		stderr: stderr.join().expect("child stderr reader panicked"),
	}
}

fn read_in_background(mut pipe: impl Read + Send + 'static) -> JoinHandle<String> {
	thread::spawn(move || {
		let mut bytes = Vec::new();
		pipe.read_to_end(&mut bytes).expect("Failed to read child output");
		String::from_utf8_lossy(&bytes).into_owned()
	})
}

fn report_field(stderr: &str, key: &str) -> Option<String> {
	let prefix = format!("{key}:");
	stderr.lines().find_map(|line| line.strip_prefix(prefix.as_str()).map(|value| value.trim().to_string()))
}
