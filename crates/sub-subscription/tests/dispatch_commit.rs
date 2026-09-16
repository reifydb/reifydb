// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
	mpsc::{Receiver, Sender, channel},
};

use reifydb::{
	Params, embedded,
	routine::abi::{
		Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
	},
	testing::db::TestDb,
};
use reifydb_core::{
	interface::catalog::{
		id::SubscriptionId,
		subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
	},
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId, value_type::ValueType};

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

	fn arity(&self) -> Arity {
		Arity::Exact(1)
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

fn drained_ids(subsystem: &SubscriptionSubsystem, sub_id: SubscriptionId) -> Vec<i32> {
	let mut out = Vec::new();
	for (_, batch) in subsystem.store().drain(&sub_id, usize::MAX) {
		let id_col = batch.iter().find(|c| c.name().text() == "id").expect("id column");
		for i in 0..batch.row_count() {
			match id_col.data().get_value(i) {
				Value::Int4(v) => out.push(v),
				other => panic!("expected Int4 id, got {:?}", other),
			}
		}
	}
	out
}

#[test]
fn a_dispatch_publishes_nothing_until_every_worker_has_finished() {
	// One commit must reach the store as one unit, or a batch splits one commit across several events.
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
		embedded::memory().with_routines(move |routines| routines.register_function(function)).build().unwrap(),
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, v: int4 }");

	let held = subscribe(&db, "from app::t filter { latch::pass(v) > 0 }");
	let free = subscribe(&db, "from app::t");
	assert_eq!(
		db.engine().spawner().pools().coordination_thread_count(),
		1,
		"both workers must share one coordination thread so the free worker's dispatch runs to completion first"
	);
	assert!(
		held.0 % 2 == 1 && free.0 % 2 == 0,
		"the free subscription must shard onto worker 0 so its dispatch is queued before the held one's \
		 (held={:?} free={:?})",
		held,
		free
	);

	latch.closed.store(true, Ordering::Release);
	db.command("INSERT app::t [{id: 1, v: 10}]");
	entered_rx
		.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
		.expect("the held worker must reach the latch while evaluating the insert");

	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let pollable_while_held = subsystem.store().pending_batches();

	latch.closed.store(false, Ordering::Release);
	release_tx.send(()).unwrap();

	assert_eq!(
		pollable_while_held, 0,
		"the free worker finished while the held worker is still evaluating the same dispatch, so nothing may \
		 be pollable yet"
	);

	let target = db.watermarks().tx().current().expect("current version");
	assert!(
		db.watermarks().cdc().wait_for_consumer(target, Duration::from_seconds(10).unwrap()),
		"the subscription consumer must finish the dispatch once the held worker is released"
	);
	assert_eq!(drained_ids(subsystem, free), vec![1], "the free subscription's row must be published");
	assert_eq!(drained_ids(subsystem, held), vec![1], "the held subscription's row must be published");
}
