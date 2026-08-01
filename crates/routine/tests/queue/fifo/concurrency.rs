// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::{
		Barrier, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, frame::frame::Frame};

const WORKERS: usize = 8;
const KEYS: usize = 16;
const PER_KEY: usize = 8;

const KEYED: &str = "CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 4, ordered_by: tenant }, retry: { attempts: 5 } }";

struct Delivery {
	item: u64,
	tenant: String,
	token: String,
	attempt: u32,
}

#[derive(Default)]
struct Observed {
	order: BTreeMap<String, Vec<u64>>,
	in_flight: BTreeSet<String>,
	completed: BTreeSet<u64>,
	overlaps: Vec<String>,
	duplicates: Vec<u64>,
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(KEYED);
	t
}

fn tenant_of(key: usize) -> String {
	format!("t{key:02}")
}

fn enqueue(t: &TestEngine, tenant: &str, id: usize) {
	t.command(&format!(r#"INSERT test::jobs [{{ id: {id}, tenant: "{tenant}" }}]"#));
}

fn claim(t: &TestEngine, worker: &str, max_n: usize) -> Vec<Delivery> {
	let frames =
		t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds(60))"#));
	let frame: &Frame = frames.first().expect("claim must always return a frame");

	let column = |name: &str| {
		frame.columns
			.iter()
			.find(|c| c.name == name)
			.unwrap_or_else(|| panic!("claim must return a {name} column"))
	};
	let (items, tenants, tokens, attempts) = (column("item"), column("tenant"), column("token"), column("attempt"));

	(0..frame.row_count())
		.map(|i| {
			let item = match items.data.get_value(i) {
				Value::Uint8(n) => n,
				other => panic!("item must be Uint8, got {other:?}"),
			};
			Delivery {
				item,
				tenant: match tenants.data.get_value(i) {
					Value::Utf8(tenant) => tenant,
					other => panic!("tenant must be Utf8, got {other:?}"),
				},
				token: match tokens.data.get_value(i) {
					Value::Utf8(t) => t,
					other => panic!("token must be Utf8, got {other:?}"),
				},
				attempt: match attempts.data.get_value(i) {
					Value::Uint4(n) => n,
					other => panic!("attempt must be Uint4, got {other:?}"),
				},
			}
		})
		.collect()
}

fn ack(t: &TestEngine, token: &str, outcome: &str) {
	t.command(&format!(r#"CALL queue::ack("{token}", "{outcome}", none)"#));
}

fn take_key(observed: &Mutex<Observed>, delivery: &Delivery) {
	let mut observed = observed.lock().unwrap();
	if !observed.in_flight.insert(delivery.tenant.clone()) {
		observed.overlaps.push(delivery.tenant.clone());
	}
	observed.order.entry(delivery.tenant.clone()).or_default().push(delivery.item);
}

fn release_key(observed: &Mutex<Observed>, delivery: &Delivery, completed: bool) {
	let mut observed = observed.lock().unwrap();
	observed.in_flight.remove(&delivery.tenant);
	if completed && !observed.completed.insert(delivery.item) {
		observed.duplicates.push(delivery.item);
	}
}

fn expected_order() -> BTreeMap<String, Vec<u64>> {
	let mut expected: BTreeMap<String, Vec<u64>> = BTreeMap::new();
	let mut row = 0u64;
	for _ in 0..PER_KEY {
		for key in 0..KEYS {
			row += 1;
			expected.entry(tenant_of(key)).or_default().push(row);
		}
	}
	expected
}

fn seed(t: &TestEngine) {
	for id in 0..PER_KEY {
		for key in 0..KEYS {
			enqueue(t, &tenant_of(key), id);
		}
	}
}

#[test]
fn test_concurrent_claim_and_ack_preserve_per_key_order() {
	let t = engine();
	seed(&t);
	let expected = expected_order();

	let observed = Mutex::new(Observed::default());
	let barrier = Barrier::new(WORKERS);

	thread::scope(|scope| {
		for worker in 0..WORKERS {
			let (t, observed, barrier) = (&t, &observed, &barrier);
			scope.spawn(move || {
				let name = format!("w{worker}");
				barrier.wait();

				let mut idle = 0;
				while idle < 3 {
					let batch = claim(t, &name, 4);
					if batch.is_empty() {
						idle += 1;
						continue;
					}
					idle = 0;

					for delivery in &batch {
						take_key(observed, delivery);
						release_key(observed, delivery, true);
						ack(t, &delivery.token, "ok");
					}
				}
			});
		}
	});

	let observed = observed.into_inner().unwrap();
	assert!(observed.overlaps.is_empty(), "two items of one key were leased at once: {:?}", observed.overlaps);
	assert!(observed.duplicates.is_empty(), "items were delivered twice: {:?}", observed.duplicates);
	assert_eq!(observed.completed.len(), KEYS * PER_KEY, "every item must be delivered exactly once");
	assert_eq!(observed.order, expected, "every key must be delivered in its enqueue order");
}

#[test]
fn test_per_key_order_survives_failed_attempts_and_redelivery() {
	let t = engine();
	seed(&t);
	let expected = expected_order();

	let observed = Mutex::new(Observed::default());
	let failed: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());
	let barrier = Barrier::new(WORKERS);

	thread::scope(|scope| {
		for worker in 0..WORKERS {
			let (t, observed, failed, barrier) = (&t, &observed, &failed, &barrier);
			scope.spawn(move || {
				let name = format!("w{worker}");
				barrier.wait();

				let mut idle = 0;
				while idle < 3 {
					let batch = claim(t, &name, 4);
					if batch.is_empty() {
						idle += 1;
						t.mock_clock().advance_millis(11_000);
						continue;
					}
					idle = 0;

					for delivery in &batch {
						let first_failure = failed.lock().unwrap().insert(delivery.item);
						let outcome = if first_failure {
							"err"
						} else {
							"ok"
						};

						if !first_failure {
							assert!(
								delivery.attempt >= 2,
								"a redelivered item must carry a higher attempt"
							);
							take_key(observed, delivery);
						}
						release_key(observed, delivery, !first_failure);
						ack(t, &delivery.token, outcome);
					}
				}
			});
		}
	});

	let observed = observed.into_inner().unwrap();
	assert!(observed.duplicates.is_empty(), "items completed twice: {:?}", observed.duplicates);
	assert_eq!(observed.completed.len(), KEYS * PER_KEY, "every item must eventually complete");
	assert_eq!(observed.order, expected, "a failed attempt must not let a younger sibling overtake");
}

#[test]
fn test_items_enqueued_during_a_drain_keep_their_key_order() {
	let t = engine();
	let expected = expected_order();

	let observed = Mutex::new(Observed::default());
	let barrier = Barrier::new(WORKERS + 1);
	let seeding = AtomicBool::new(true);

	thread::scope(|scope| {
		let (enqueuer, barrier_ref, seeding_ref) = (&t, &barrier, &seeding);
		scope.spawn(move || {
			barrier_ref.wait();
			seed(enqueuer);
			seeding_ref.store(false, Ordering::Release);
		});

		for worker in 0..WORKERS {
			let (t, observed, barrier, seeding) = (&t, &observed, &barrier, &seeding);
			scope.spawn(move || {
				let name = format!("w{worker}");
				barrier.wait();

				let mut idle = 0;
				while idle < 3 {
					let batch = claim(t, &name, 4);
					if batch.is_empty() {
						if seeding.load(Ordering::Acquire) {
							thread::yield_now();
							continue;
						}
						idle += 1;
						continue;
					}
					idle = 0;

					for delivery in &batch {
						take_key(observed, delivery);
						release_key(observed, delivery, true);
						ack(t, &delivery.token, "ok");
					}
				}
			});
		}
	});

	let observed = observed.into_inner().unwrap();
	assert!(observed.overlaps.is_empty(), "two items of one key were leased at once: {:?}", observed.overlaps);
	assert!(observed.duplicates.is_empty(), "items were delivered twice: {:?}", observed.duplicates);
	assert_eq!(observed.completed.len(), KEYS * PER_KEY, "every enqueued item must be drained");
	assert_eq!(observed.order, expected, "concurrent enqueues must still deliver each key in insert order");
}

#[test]
fn test_a_claim_never_hands_out_an_item_whose_payload_it_cannot_read() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE diag");
	t.admin("CREATE QUEUE diag::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 4 } }");

	let missing = Mutex::new(0usize);
	let barrier = Barrier::new(2);
	thread::scope(|scope| {
		let (e, b) = (&t, &barrier);
		scope.spawn(move || {
			b.wait();
			for id in 0..200 {
				e.command(&format!(r#"INSERT diag::jobs [{{ id: {id}, tenant: "x" }}]"#));
			}
		});
		let (t, b, missing) = (&t, &barrier, &missing);
		scope.spawn(move || {
			b.wait();
			let mut idle = 0;
			while idle < 50 {
				let frames =
					t.command(r#"CALL queue::claim("w", "diag::jobs", 4, duration::seconds(60))"#);
				let frame = frames.first().unwrap();
				if frame.row_count() == 0 {
					idle += 1;
					continue;
				}
				idle = 0;
				let tenants = frame.columns.iter().find(|c| c.name == "tenant").unwrap();
				for i in 0..frame.row_count() {
					if matches!(tenants.data.get_value(i), Value::None { .. }) {
						*missing.lock().unwrap() += 1;
					}
				}
			}
		});
	});
	assert_eq!(*missing.lock().unwrap(), 0, "claim handed out items whose payload its snapshot cannot see");
}
