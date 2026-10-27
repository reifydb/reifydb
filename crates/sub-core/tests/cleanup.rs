// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{future::Future, sync::Arc};

use reifydb_codec::wire::WireFormat as ClientWireFormat;
use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{SubscribeOptions, SubscribeOutcome},
};
use reifydb_sub_core::{
	handler::{SubscribeError, handle_subscribe, handle_unsubscribe},
	host::{SubscribeContext, SubscribeHost},
	registry::SubscriptionRegistry,
	wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Error,
	params::Params,
	value::{duration::Duration, frame::frame::Frame, identity::IdentityId, uuid::Uuid7},
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

	fn send_subscribed(&self, _subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		self.result()
	}

	fn send_change(&self, _format: (), _entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		match self.result() {
			DeliveryResult::Delivered => ChangeDelivery::Delivered {
				failed: Vec::new(),
			},
			DeliveryResult::Disconnected => ChangeDelivery::Disconnected,
		}
	}

	fn send_subscription_closed(&self, _subscription_id: SubscriptionId) -> DeliveryResult {
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
fn an_unsubscribe_that_cannot_unregister_its_subscriptions_fails() {
	// The engine keeps staging changes for a subscription it failed to drop, so this must never succeed.
	let engine = TestEngine::new();
	let (clock, rng) = (engine.inner().clock().clone(), engine.inner().rng().clone());
	let registry = Arc::new(SubscriptionRegistry::new(clock.clone()));
	let connection_id = Uuid7::generate(&clock, &rng);
	registry.subscribe(
		SubscriptionId(7),
		connection_id,
		TestSink {
			open: true,
		},
		(),
		None,
		Duration::zero(),
		Duration::zero(),
	);

	let error = block_on(handle_unsubscribe(engine.inner(), &registry, connection_id, &[SubscriptionId(7)]))
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
		&[("8".to_string(), Params::None, SubscribeOptions::default())],
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
		&[("9".to_string(), Params::None, options)],
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
fn a_rollback_that_cannot_unregister_an_earlier_subscription_fails() {
	// A partial rollback the engine could not apply must never hide behind the rejected query.
	let engine = TestEngine::new();
	let host = TestHost::new(&engine);
	let registry = Arc::new(SubscriptionRegistry::new(engine.inner().clock().clone()));
	let connection_id = Uuid7::generate(engine.inner().clock(), engine.inner().rng());
	let (_shutdown_tx, shutdown) = watch::channel(false);
	let queries = vec![
		("10".to_string(), Params::None, SubscribeOptions::default()),
		("not a subscription".to_string(), Params::None, SubscribeOptions::default()),
	];

	let outcome = block_on(handle_subscribe(
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

#[test]
fn an_empty_subscribe_is_refused_without_touching_the_registry() {
	// An empty call that reaches registration acks nothing yet may leave a connection entry behind.
	let engine = TestEngine::new();
	let host = TestHost::new(&engine);
	let registry = Arc::new(SubscriptionRegistry::new(engine.inner().clock().clone()));
	let connection_id = Uuid7::generate(engine.inner().clock(), engine.inner().rng());
	let (_shutdown_tx, shutdown) = watch::channel(false);

	let outcome = block_on(handle_subscribe(
		&host,
		connection_id,
		IdentityId::root(),
		&[],
		TestSink {
			open: true,
		},
		&registry,
		(),
		shutdown,
	))
	.expect("an empty call fails as a client error, not an unexpected one");

	assert!(matches!(outcome, Err(SubscribeError::Empty)), "an empty call must be refused as empty");
	assert!(registry.active_subscriptions().is_empty());
	assert!(registry.cleanup_connection(connection_id).is_empty(), "no connection entry may be left behind");
}
