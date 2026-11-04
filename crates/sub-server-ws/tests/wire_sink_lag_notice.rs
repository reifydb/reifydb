// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::subscription::subscription_lagged, interface::catalog::id::SubscriptionId,
	value::batch::single_row,
};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::{
	response::ServerPush,
	subscription::registry::{PushMessage, SubscriptionRegistry, WsWireSink},
};
use reifydb_subscription::delivery::SubscriptionDelivery;
use reifydb_value::value::{Value, diff_type::DiffType, duration::Duration, uuid::Uuid7};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::sync::mpsc::unbounded_channel;

#[test]
fn a_lagged_subscription_is_closed_with_its_diagnostic_and_no_pending_rows() {
	// Without the diagnostic a lagged browser reports a generic close and never learns it must resubscribe.
	let (push_tx, mut push_rx) = unbounded_channel();
	let clock = Clock::Mock(MockClock::from_millis(1000));
	let registry = SubscriptionRegistry::new(clock.clone());
	let id = SubscriptionId(7);
	registry.subscribe(
		id,
		Uuid7::generate(&clock, &Rng::seeded(1)),
		WsWireSink::new(push_tx),
		WireFormat::Json,
		None,
		Duration::zero(),
		Duration::zero(),
	);
	registry.try_deliver(&id, DiffType::Insert, single_row([("v", Value::Int8(1))]).unwrap());
	let diagnostic = subscription_lagged(7, 1024, 3);

	registry.terminate(&id, diagnostic.clone());

	let (subscription_id, reason) = match push_rx.try_recv() {
		Ok(PushMessage::SubscriptionClosed {
			subscription_id,
			reason,
		}) => (subscription_id, reason),
		other => panic!("a lagged subscription must be closed before anything else, got {other:?}"),
	};
	assert_eq!(subscription_id, id);
	assert_eq!(reason.as_ref(), Some(&diagnostic), "the close must carry the lag diagnostic unchanged");
	assert!(push_rx.try_recv().is_err(), "the missed rows are gone, so nothing may follow the close");
	assert!(registry.active_subscriptions().is_empty(), "the closed subscription must be forgotten");

	let wire: JsonValue =
		from_str(&ServerPush::subscription_closed(subscription_id.to_string(), reason).to_json()).unwrap();
	assert_eq!(wire["payload"]["reason"]["code"], json!("SUBS_005"));
	assert_eq!(wire["payload"]["reason"]["help"], json!(diagnostic.help));
}
