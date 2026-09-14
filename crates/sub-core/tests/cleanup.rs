// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{future::Future, sync::Arc};

use reifydb_codec::wire::{RawChangePayload, WireFormat as ClientWireFormat};
use reifydb_core::{
	interface::catalog::{
		id::SubscriptionId,
		subscription::{SubscribeOptions, SubscribeOutcome},
	},
	value::column::columns::Columns,
};
use reifydb_sub_core::{
	handler::{handle_batch_subscribe, handle_batch_unsubscribe, handle_subscribe},
	host::{SubscribeContext, SubscribeHost},
	registry::SubscriptionRegistry,
	wire_sink::{BatchSubscribedEntry, WireSink},
};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Error,
	params::Params,
	value::{diff_type::DiffType, duration::Duration, frame::frame::Frame, identity::IdentityId, uuid::Uuid7},
};
use tokio::{runtime::Builder, sync::watch};

#[derive(Clone)]
struct TestSink {
	open: bool,
}

impl TestSink {
	fn result(&self) -> DeliveryResult {
		if self.open {
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}
}

impl WireSink for TestSink {
	type Format = ();

	fn client_wire_format(_format: ()) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		self.result()
	}

	fn send_batch_subscribed(&self, _batch_id: BatchId, _subscriptions: &[BatchSubscribedEntry]) -> DeliveryResult {
		self.result()
	}

	fn send_change(
		&self,
		_sub_id: SubscriptionId,
		_op: DiffType,
		_columns: Columns,
		_format: (),
	) -> DeliveryResult {
		self.result()
	}

	fn send_remote_change(
		&self,
		_sub_id: SubscriptionId,
		_payload: RawChangePayload,
		_format: (),
	) -> DeliveryResult {
		self.result()
	}

	fn send_batch_envelope(
		&self,
		_batch_id: BatchId,
		_format: (),
		_entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		self.result()
	}

	fn send_batch_subscription_closed(&self, _batch_id: BatchId, _sub_id: SubscriptionId) -> DeliveryResult {
		self.result()
	}

	fn send_closed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		self.result()
	}
}

struct TestHost {
	context: SubscribeContext,
}

impl TestHost {
	fn new(engine: &TestEngine) -> Self {
		let engine = engine.inner();
		Self {
			context: SubscribeContext::new(
				engine.clone(),
				engine.clock().clone(),
				engine.rng().clone(),
				100,
				Duration::zero(),
				Duration::zero(),
			),
		}
	}
}

impl SubscribeHost for TestHost {
	type Error = String;

	fn context(&self) -> &SubscribeContext {
		&self.context
	}

	async fn execute_subscribe(
		&self,
		_identity: IdentityId,
		query: String,
		_params: Params,
		_options: SubscribeOptions,
	) -> Result<SubscribeOutcome, String> {
		match query.parse::<u64>() {
			Ok(id) => Ok(SubscribeOutcome::Local {
				id: SubscriptionId(id),
			}),
			Err(_) => Err(format!("rejected query {query}")),
		}
	}
}

fn block_on<F: Future>(future: F) -> F::Output {
	Builder::new_current_thread().enable_all().build().expect("test runtime").block_on(future)
}

fn assert_names_subscription(error: &Error, subscription_id: u64) {
	let expected = format!("could not unregister subscription {subscription_id}");
	assert!(error.0.message.contains(&expected), "expected `{expected}` in: {}", error.0.message);
}

#[test]
fn a_batch_unsubscribe_that_cannot_unregister_its_subscriptions_fails() {
	// The engine keeps staging changes for a subscription it failed to drop, so this must never succeed.
	let engine = TestEngine::new();
	let (clock, rng) = (engine.inner().clock().clone(), engine.inner().rng().clone());
	let registry = Arc::new(SubscriptionRegistry::new(clock.clone()));
	let connection_id = Uuid7::generate(&clock, &rng);
	let sink = TestSink {
		open: true,
	};
	registry.subscribe(
		SubscriptionId(7),
		connection_id,
		sink.clone(),
		(),
		None,
		Duration::zero(),
		Duration::zero(),
	);
	let batch_id = registry.register_batch(
		connection_id,
		vec![(SubscriptionId(7), Duration::zero())],
		sink,
		(),
		&clock,
		&rng,
	);

	let error = block_on(handle_batch_unsubscribe(engine.inner(), &registry, connection_id, batch_id))
		.expect_err("the engine has no subscription service, so unregistering must fail");

	assert_names_subscription(&error, 7);
}

#[test]
fn a_subscribe_whose_stream_closed_fails_when_it_cannot_unregister() {
	// A rollback the engine could not apply must never hide behind the stream-closed client error.
	let engine = TestEngine::new();
	let host = TestHost::new(&engine);
	let registry = Arc::new(SubscriptionRegistry::new(engine.inner().clock().clone()));
	let connection_id = Uuid7::generate(engine.inner().clock(), engine.inner().rng());
	let (_shutdown_tx, shutdown) = watch::channel(false);

	let outcome = block_on(handle_subscribe(
		&host,
		connection_id,
		IdentityId::root(),
		"8".to_string(),
		Params::None,
		SubscribeOptions::default(),
		TestSink {
			open: false,
		},
		&registry,
		(),
		shutdown,
	));

	let Err(error) = outcome else {
		panic!("a failed rollback must surface as an unexpected error, not as a client error");
	};
	assert_names_subscription(&error, 8);
}

#[test]
fn a_failed_hydration_fails_when_it_cannot_unregister_the_subscription() {
	// An abort the engine could not apply must never hide behind the hydration client error.
	let engine = TestEngine::new();
	let host = TestHost::new(&engine);
	let registry = Arc::new(SubscriptionRegistry::new(engine.inner().clock().clone()));
	let connection_id = Uuid7::generate(engine.inner().clock(), engine.inner().rng());
	let (_shutdown_tx, shutdown) = watch::channel(false);
	let options = SubscribeOptions::default();
	assert!(options.hydration.enabled, "precondition: the subscription must hydrate to reach the abort");

	let outcome = block_on(handle_subscribe(
		&host,
		connection_id,
		IdentityId::root(),
		"9".to_string(),
		Params::None,
		options,
		TestSink {
			open: true,
		},
		&registry,
		(),
		shutdown,
	));

	let Err(error) = outcome else {
		panic!("a failed abort must surface as an unexpected error, not as a hydration error");
	};
	assert_names_subscription(&error, 9);
}

#[test]
fn a_batch_rollback_that_cannot_unregister_an_earlier_subscription_fails() {
	// A partial batch rollback the engine could not apply must never hide behind the rejected query.
	let engine = TestEngine::new();
	let host = TestHost::new(&engine);
	let registry = Arc::new(SubscriptionRegistry::new(engine.inner().clock().clone()));
	let connection_id = Uuid7::generate(engine.inner().clock(), engine.inner().rng());
	let (_shutdown_tx, shutdown) = watch::channel(false);
	let queries = vec![
		("10".to_string(), Params::None, SubscribeOptions::default()),
		("not a subscription".to_string(), Params::None, SubscribeOptions::default()),
	];

	let outcome = block_on(handle_batch_subscribe(
		&host,
		connection_id,
		IdentityId::root(),
		&queries,
		TestSink {
			open: true,
		},
		&registry,
		(),
		shutdown,
	));

	let Err(error) = outcome else {
		panic!("a failed rollback must surface as an unexpected error, not as the rejected query");
	};
	assert_names_subscription(&error, 10);
}
