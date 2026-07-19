// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::error::Error;

use reifydb::runtime::context::clock::MockClock;
use reifydb_client::{ChangeKind, ChangePayload, HydrationConfig, SubscriptionConfig};
use reifydb_value::value::duration::Duration;

use super::{SubscriptionTestHarness, TestContext, recv_with_timeout};

fn ms(m: u64) -> Duration {
	Duration::from_milliseconds(m as i64).unwrap()
}

fn config(hydration: bool, throttle: Option<u64>, linger: Option<u64>) -> SubscriptionConfig {
	SubscriptionConfig {
		hydration: HydrationConfig {
			enabled: hydration,
			max_rows: None,
		},
		throttle: throttle.map(ms),
		linger: linger.map(ms),
	}
}

// Drive delivery by stepping the mock clock. An immediate (unshaped or throttle
// leading-edge) change arrives on the first poll; a buffered (linger) change arrives once
// an advance carries the clock past `first_pending_at + window`. Advancing in a loop is
// robust to the async gap between the insert committing and the poller buffering it: each
// step is larger than any window used here, so one advance after buffering makes the gate
// ready regardless of exactly when buffering landed.
async fn recv_after_advancing(ctx: &mut TestContext, clock: &MockClock) -> Option<ChangePayload> {
	for _ in 0..40 {
		if let Some(change) = recv_with_timeout(&mut ctx.client, 50).await {
			return Some(change);
		}
		clock.advance_millis(500);
	}
	None
}

// Delivery is the invariant across every shaping combination: throttle and linger only
// change *when* a change is pushed, never *whether* a lone change is eventually delivered.
async fn assert_isolated_insert_delivers(
	mut ctx: TestContext,
	clock: MockClock,
	config: SubscriptionConfig,
	label: &str,
) -> Result<(), Box<dyn Error>> {
	let table = ctx.create_table("shape", "id: int4, name: utf8").await?;
	let sub_id = ctx.subscribe(&table, config).await?;

	ctx.insert(&table, "{ id: 1, name: 'a' }").await?;

	let change = recv_after_advancing(&mut ctx, &clock)
		.await
		.unwrap_or_else(|| panic!("[{label}] expected the isolated insert to be delivered"));

	assert_eq!(change.changes.len(), 1, "[{label}] expected exactly one frame");
	assert_eq!(change.changes[0].kind, ChangeKind::Insert, "[{label}] expected an insert");

	ctx.close(&sub_id).await
}

macro_rules! delivery_case {
	($name:ident, hydration = $hydration:expr, throttle = $throttle:expr, linger = $linger:expr) => {
		#[test]
		fn $name() {
			SubscriptionTestHarness::run_with_clock(|ctx, clock| async move {
				assert_isolated_insert_delivers(
					ctx,
					clock,
					config($hydration, $throttle, $linger),
					stringify!($name),
				)
				.await
			});
		}
	};
}

delivery_case!(deliver_plain, hydration = true, throttle = None, linger = None);
delivery_case!(deliver_hydration_off, hydration = false, throttle = None, linger = None);
delivery_case!(deliver_throttle_only, hydration = true, throttle = Some(150), linger = None);
delivery_case!(deliver_linger_only, hydration = true, throttle = None, linger = Some(200));
delivery_case!(deliver_throttle_and_linger, hydration = true, throttle = Some(150), linger = Some(200));
delivery_case!(deliver_hydration_off_throttle, hydration = false, throttle = Some(150), linger = None);
delivery_case!(deliver_hydration_off_linger, hydration = false, throttle = None, linger = Some(200));
delivery_case!(deliver_hydration_off_both, hydration = false, throttle = Some(150), linger = Some(200));

// Hydration replays rows that already exist at subscribe time; disabling it skips that
// replay while still delivering subsequent live changes. Neither path is time-shaped, so
// these run on the default (frozen) clock without advancing.
#[test]
fn hydration_enabled_replays_existing_rows() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("hydrate", "id: int4, name: utf8").await?;
		ctx.insert(&table, "{ id: 7, name: 'seed' }").await?;

		let sub_id = ctx.subscribe(&table, config(true, None, None)).await?;

		let change = recv_with_timeout(&mut ctx.client, 3000)
			.await
			.expect("hydration should replay the pre-existing row");
		assert_eq!(change.changes.len(), 1, "the seeded row should be replayed as one frame");

		ctx.close(&sub_id).await
	});
}

#[test]
fn hydration_disabled_skips_replay() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("no_hydrate", "id: int4, name: utf8").await?;
		ctx.insert(&table, "{ id: 7, name: 'seed' }").await?;

		let sub_id = ctx.subscribe(&table, config(false, None, None)).await?;

		let replayed = recv_with_timeout(&mut ctx.client, 500).await;
		assert!(replayed.is_none(), "disabled hydration must not replay the pre-existing row");

		ctx.insert(&table, "{ id: 8, name: 'live' }").await?;
		let change = recv_with_timeout(&mut ctx.client, 3000)
			.await
			.expect("a live insert must still be delivered when hydration is disabled");
		assert_eq!(change.changes[0].kind, ChangeKind::Insert);

		ctx.close(&sub_id).await
	});
}
