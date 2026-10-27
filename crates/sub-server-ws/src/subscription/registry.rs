// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	json::to::convert_frames,
	wire::WireFormat as ClientWireFormat,
};
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_core::{
	envelope::encode_rbcf_change_envelope,
	wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
};
use reifydb_sub_server::{
	format::WireFormat,
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, resolve_change_json},
};
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::{frame::frame::Frame, uuid::Uuid7};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::sync::mpsc;
use tracing::{instrument, warn};

use crate::response::ChangeEntry;

pub type ConnectionId = Uuid7;

pub type SubscriptionRegistry = reifydb_sub_core::registry::SubscriptionRegistry<WsWireSink>;

#[derive(Debug, Clone)]
pub enum PushMessage {
	ChangeJson {
		entries: Vec<ChangeEntry>,
	},

	ChangeRbcf {
		envelope: Vec<u8>,
	},

	SubscriptionClosed {
		subscription_id: SubscriptionId,
	},
}

#[derive(Clone)]
pub struct WsWireSink {
	pub push_tx: mpsc::UnboundedSender<PushMessage>,
}

impl WsWireSink {
	pub fn new(push_tx: mpsc::UnboundedSender<PushMessage>) -> Self {
		Self {
			push_tx,
		}
	}
}

impl WireSink for WsWireSink {
	type Format = WireFormat;

	fn client_wire_format(format: Self::Format) -> ClientWireFormat {
		match format {
			WireFormat::Rbcf => ClientWireFormat::Rbcf,
			WireFormat::Json | WireFormat::Frames => ClientWireFormat::Rbcf,
		}
	}

	fn send_subscribed(&self, _subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	#[instrument(name = "server::ws_encode", level = "trace", skip_all, fields(entries = entries.len()))]
	fn send_change(&self, format: Self::Format, entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		let (message, failed) = encode_entries(format, entries);
		if let Some(message) = message
			&& self.push_tx.send(message).is_err()
		{
			return ChangeDelivery::Disconnected;
		}
		ChangeDelivery::Delivered {
			failed,
		}
	}

	fn send_subscription_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
		if self.push_tx
			.send(PushMessage::SubscriptionClosed {
				subscription_id,
			})
			.is_ok()
		{
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}
}

pub fn encode_entries(
	format: WireFormat,
	entries: Vec<(SubscriptionId, Vec<Frame>)>,
) -> (Option<PushMessage>, Vec<SubscriptionId>) {
	let mut failed = Vec::new();
	let message = match format {
		WireFormat::Rbcf => {
			let mut encoded = Vec::with_capacity(entries.len());
			for (sub_id, frames) in entries {
				match encode_frames(&frames, &EncodeOptions::fast()) {
					Ok(rbcf) => encoded.push((sub_id.to_string(), rbcf)),
					Err(e) => {
						warn!("Failed to RBCF-encode change for {}, closing it: {}", sub_id, e);
						failed.push(sub_id);
					}
				}
			}
			(!encoded.is_empty()).then(|| PushMessage::ChangeRbcf {
				envelope: encode_rbcf_change_envelope(&encoded),
			})
		}
		WireFormat::Frames | WireFormat::Json => {
			let mut encoded = Vec::with_capacity(entries.len());
			for (sub_id, frames) in entries {
				match encode_json_entry(format, sub_id, frames) {
					Ok(entry) => encoded.push(entry),
					Err(e) => {
						warn!("Failed to encode change for {}, closing it: {}", sub_id, e);
						failed.push(sub_id);
					}
				}
			}
			(!encoded.is_empty()).then(|| PushMessage::ChangeJson {
				entries: encoded,
			})
		}
	};
	(message, failed)
}

fn encode_json_entry(format: WireFormat, sub_id: SubscriptionId, frames: Vec<Frame>) -> Result<ChangeEntry, String> {
	Ok(match format {
		WireFormat::Json => {
			let resolved = resolve_change_json(frames)?;
			ChangeEntry {
				subscription_id: sub_id.to_string(),
				content_type: CONTENT_TYPE_JSON.to_string(),
				body: from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body)),
			}
		}
		WireFormat::Frames | WireFormat::Rbcf => ChangeEntry {
			subscription_id: sub_id.to_string(),
			content_type: CONTENT_TYPE_FRAMES.to_string(),
			body: json!({ "frames": convert_frames(&frames).map_err(|e| e.to_string())? }),
		},
	})
}

#[cfg(test)]
pub mod tests {
	use std::{collections::HashSet, sync::Arc};

	use arrow_array::{RecordBatch, UInt64Array};
	use reifydb_codec::frame::decode::decode_frames;
	use reifydb_core::{interface::catalog::id::SubscriptionId, value::batch::single_row};
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_sub_core::registry::PromoteResult;
	use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
	use reifydb_value::value::{
		Value,
		diff_type::DiffType,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{SystemColumn, row_numbers, with_system_column},
		uuid::Uuid7,
	};

	use super::*;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	fn single_int_columns(name: &str, value: i64) -> RecordBatch {
		single_row([(name, Value::Int8(value))]).unwrap()
	}

	fn columns_with_row_number(id: i64, row_number: u64) -> RecordBatch {
		with_system_column(
			single_row([("id", Value::Int8(id))]).unwrap(),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![row_number])),
		)
		.unwrap()
	}

	fn encode_one(sub_id: SubscriptionId, op: DiffType, columns: RecordBatch, format: WireFormat) -> PushMessage {
		let (message, failed) = encode_entries(format, vec![(sub_id, vec![Frame::from(columns).with_op(op)])]);
		assert!(failed.is_empty(), "a well formed change must encode");
		message.expect("one encoded entry yields one push")
	}

	fn entries_of(message: PushMessage) -> Vec<ChangeEntry> {
		match message {
			PushMessage::ChangeJson {
				entries,
			} => entries,
			other => panic!("expected a json push message, got {:?}", other),
		}
	}

	fn push_body(message: PushMessage) -> JsonValue {
		let mut entries = entries_of(message);
		assert_eq!(entries.len(), 1, "one change encodes to one entry");
		entries.remove(0).body
	}

	fn push_rbcf(message: PushMessage) -> Vec<u8> {
		match message {
			PushMessage::ChangeRbcf {
				envelope,
			} => envelope,
			other => panic!("expected an rbcf push message, got {:?}", other),
		}
	}

	fn registry_with_sink()
	-> (MockClock, SubscriptionRegistry, Uuid7, WsWireSink, mpsc::UnboundedReceiver<PushMessage>) {
		let (mock, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, push_rx) = mpsc::unbounded_channel();
		(mock, registry, connection_id, WsWireSink::new(push_tx), push_rx)
	}

	fn subscribe_shaped(
		registry: &SubscriptionRegistry,
		sub: SubscriptionId,
		connection_id: Uuid7,
		sink: &WsWireSink,
		throttle: i64,
		linger: i64,
	) {
		registry.subscribe(
			sub,
			connection_id,
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::from_milliseconds(throttle).unwrap(),
			Duration::from_milliseconds(linger).unwrap(),
		);
	}

	#[test]
	fn every_wire_format_reports_the_same_row_number_and_op() {
		// A client keys its local state on the row number, so a format that drops it or renames it
		// would silently strip the only stable identity a row has. `id` here is a user column that
		// deliberately differs from the row number: the two must never be conflated.
		let sub_id = SubscriptionId(7);
		let op = DiffType::Update;

		let json = push_body(encode_one(sub_id, op, columns_with_row_number(500, 42), WireFormat::Json));
		let envelope = json.as_array().expect("json format encodes one envelope per frame");
		assert_eq!(envelope.len(), 1);
		assert_eq!(envelope[0]["op"], JsonValue::from(DiffType::as_u8(op)));
		let json_rows = envelope[0]["rows"].as_array().expect("envelope carries rows");
		assert_eq!(json_rows.len(), 1);
		assert_eq!(json_rows[0][SystemColumn::RowNumbers.name()], JsonValue::from(42u64));
		assert_eq!(json_rows[0]["id"], JsonValue::from("500"));

		let frames = push_body(encode_one(sub_id, op, columns_with_row_number(500, 42), WireFormat::Frames));
		let frame = &frames["frames"].as_array().expect("frames format carries a frame list")[0];
		assert_eq!(frame["op"], JsonValue::from(DiffType::as_u8(op)));
		let row_number_column = frame["columns"]
			.as_array()
			.expect("frames format carries a column list")
			.iter()
			.find(|column| column["name"] == SystemColumn::RowNumbers.name())
			.expect("frames format carries #rownum as an ordinary column");
		assert_eq!(row_number_column["payload"], JsonValue::from(vec!["42"]));

		let payload = encode_frames(
			&[Frame::from(columns_with_row_number(500, 42)).with_op(op)],
			&EncodeOptions::fast(),
		)
		.unwrap();
		let rbcf = push_rbcf(encode_one(sub_id, op, columns_with_row_number(500, 42), WireFormat::Rbcf));
		assert!(rbcf.ends_with(&payload), "the envelope must carry the rbcf payload verbatim");
		let decoded = decode_frames(&payload).expect("rbcf payload must decode");
		assert_eq!(decoded.len(), 1);
		assert_eq!(decoded[0].op, Some(op));
		assert_eq!(row_numbers(&decoded[0].batch).unwrap(), &[RowNumber::new(42)]);
	}

	#[test]
	fn a_user_column_named_id_never_shadows_the_row_number() {
		// `id` is an ordinary user column and may repeat, be absent, or be reused. The row number is
		// the server's identity and must survive independently of it, or a client keyed on identity
		// would collapse two distinct rows into one.
		let columns = columns_with_row_number(1, 99);
		let json = push_body(encode_one(SubscriptionId(1), DiffType::Insert, columns, WireFormat::Json));
		let row = &json.as_array().unwrap()[0]["rows"].as_array().unwrap()[0];

		assert_eq!(row["id"], JsonValue::from("1"));
		assert_eq!(row[SystemColumn::RowNumbers.name()], JsonValue::from(99u64));
		assert_ne!(row["id"], row[SystemColumn::RowNumbers.name()]);
	}

	#[test]
	fn insert_update_remove_report_the_same_row_number() {
		// A subscriber must be able to follow one entity across its whole lifetime: the identity it
		// inserted under must be the identity it later updates and finally removes, or the client
		// leaks the row or deletes the wrong one.
		let sub_id = SubscriptionId(3);
		let row_number = RowNumber::new(1234);

		let ops = [DiffType::Insert, DiffType::Update, DiffType::Remove];
		let seen: Vec<(u8, u64)> = ops
			.iter()
			.map(|op| {
				let columns = columns_with_row_number(7, row_number.value());
				let json = push_body(encode_one(sub_id, *op, columns, WireFormat::Json));
				let envelope = &json.as_array().unwrap()[0];
				let rn = envelope["rows"].as_array().unwrap()[0][SystemColumn::RowNumbers.name()]
					.as_u64()
					.expect("every row carries its row number");
				(envelope["op"].as_u64().unwrap() as u8, rn)
			})
			.collect();

		assert_eq!(seen, vec![(1, 1234), (2, 1234), (3, 1234)]);
	}

	#[tokio::test]
	async fn test_subscribe_unsubscribe() {
		let (_, registry, connection_id, sink, _rx) = registry_with_sink();

		let sub_id = SubscriptionId(12345);
		registry.subscribe(
			sub_id,
			connection_id,
			sink,
			WireFormat::Frames,
			None,
			Duration::zero(),
			Duration::zero(),
		);
		assert_eq!(registry.active_subscriptions().len(), 1);

		assert!(registry.unsubscribe(sub_id));
		assert_eq!(registry.active_subscriptions().len(), 0);
		assert!(registry.cleanup_connection(connection_id).is_empty());

		assert!(!registry.unsubscribe(sub_id));
	}

	#[tokio::test]
	async fn test_cleanup_connection() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx1, _rx1) = mpsc::unbounded_channel();
		let (tx2, _rx2) = mpsc::unbounded_channel();

		let sub1 = SubscriptionId(12345);
		let sub2 = SubscriptionId(12346);
		registry.subscribe(
			sub1,
			connection_id,
			WsWireSink::new(tx1),
			WireFormat::Json,
			None,
			Duration::zero(),
			Duration::zero(),
		);
		registry.subscribe(
			sub2,
			connection_id,
			WsWireSink::new(tx2),
			WireFormat::Json,
			None,
			Duration::zero(),
			Duration::zero(),
		);
		assert_eq!(registry.active_subscriptions().len(), 2);

		registry.cleanup_connection(connection_id);
		assert_eq!(registry.active_subscriptions().len(), 0);
		assert!(registry.cleanup_connection(connection_id).is_empty());
	}

	#[tokio::test]
	async fn one_flush_coalesces_two_subscriptions_of_a_connection() {
		let (_, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);
		subscribe_shaped(&registry, sub_a, connection_id, &sink, 0, 0);
		subscribe_shaped(&registry, sub_b, connection_id, &sink, 0, 0);

		assert!(matches!(
			registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("value", 10)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub_b, DiffType::Insert, single_int_columns("value", 20)),
			DeliveryResult::Delivered
		));

		assert!(push_rx.try_recv().is_err());

		registry.flush();

		let msg = push_rx.try_recv().expect("expected one Change after flush");
		assert!(push_rx.try_recv().is_err());
		let entries = entries_of(msg);
		assert_eq!(entries.len(), 2);
		let ids: HashSet<String> = entries.iter().map(|e| e.subscription_id.clone()).collect();
		assert!(ids.contains(&sub_a.to_string()));
		assert!(ids.contains(&sub_b.to_string()));
	}

	#[tokio::test]
	async fn one_flush_merges_repeated_subscription_deliveries() {
		let (_, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub_a = SubscriptionId(100);
		subscribe_shaped(&registry, sub_a, connection_id, &sink, 0, 0);

		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("value", 1));
		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("value", 2));

		registry.flush();

		let entries = entries_of(push_rx.try_recv().expect("envelope"));
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].subscription_id, sub_a.to_string());
		let frames = entries[0].body.get("frames").expect("frames key").as_array().unwrap();
		assert_eq!(frames.len(), 2);
	}

	#[tokio::test]
	async fn test_warming_buffers_until_promote() {
		let (_, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub = SubscriptionId(7001);
		registry.subscribe(
			sub,
			connection_id,
			sink,
			WireFormat::Frames,
			Some(16),
			Duration::zero(),
			Duration::zero(),
		);

		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 2)),
			DeliveryResult::Delivered
		));
		registry.flush();
		assert!(push_rx.try_recv().is_err(), "no pushes while warming");

		match registry.promote_to_live(sub) {
			PromoteResult::Promoted(n) => assert_eq!(n, 2),
			other => panic!("unexpected promote result: {:?}", other),
		}
		assert!(push_rx.try_recv().is_err(), "promote queues the buffered changes for the next flush");

		registry.flush();
		let entries = entries_of(push_rx.try_recv().expect("expected the buffered changes after the flush"));
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].subscription_id, sub.to_string());
		assert_eq!(
			entries[0].body["frames"].as_array().unwrap().len(),
			2,
			"both buffered changes, in one entry"
		);
		assert!(push_rx.try_recv().is_err());

		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 3)),
			DeliveryResult::Delivered
		));
		assert!(push_rx.try_recv().is_err(), "a live change waits for the next flush");
		registry.flush();
		let entries = entries_of(push_rx.try_recv().expect("expected live push after promote"));
		assert_eq!(entries[0].subscription_id, sub.to_string());
	}

	#[tokio::test]
	async fn test_warming_overflow_marks_subscription() {
		let (_, registry, connection_id, sink, _push_rx) = registry_with_sink();

		let sub = SubscriptionId(7002);
		registry.subscribe(
			sub,
			connection_id,
			sink,
			WireFormat::Frames,
			Some(2),
			Duration::zero(),
			Duration::zero(),
		);

		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1));
		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 2));
		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 3));

		match registry.promote_to_live(sub) {
			PromoteResult::Overflowed => {}
			other => panic!("expected Overflowed, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_promote_unknown_subscription() {
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(Clock::Mock(MockClock::from_millis(0)));
		match registry.promote_to_live(SubscriptionId(999)) {
			PromoteResult::NotFound => {}
			other => panic!("expected NotFound, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_linger_respects_each_subscription_independently_not_max() {
		let (mock, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub_short = SubscriptionId(1);
		let sub_long = SubscriptionId(2);
		subscribe_shaped(&registry, sub_short, connection_id, &sink, 0, 5);
		subscribe_shaped(&registry, sub_long, connection_id, &sink, 0, 50);

		registry.try_deliver(&sub_short, DiffType::Insert, single_int_columns("v", 1));
		registry.try_deliver(&sub_long, DiffType::Insert, single_int_columns("v", 2));

		registry.flush();
		assert!(push_rx.try_recv().is_err(), "no subscription is due before its own linger elapses");

		mock.advance_millis(5);
		registry.flush();
		let entries = entries_of(push_rx.try_recv().expect("the short-linger subscription is due"));
		assert_eq!(
			entries.len(),
			1,
			"only the short-linger subscription flushes - lingers are respected individually, never max-ed across the connection"
		);
		assert_eq!(entries[0].subscription_id, sub_short.to_string());
		assert!(
			push_rx.try_recv().is_err(),
			"the long-linger subscription is held back, not dragged out early by the short one"
		);

		mock.advance_millis(50);
		registry.flush();
		let entries = entries_of(push_rx.try_recv().expect("the long-linger subscription is now due"));
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].subscription_id, sub_long.to_string());
	}

	#[tokio::test]
	async fn test_linger_concatenates_changes_within_its_window() {
		let (mock, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub_a = SubscriptionId(7);
		subscribe_shaped(&registry, sub_a, connection_id, &sink, 0, 10);

		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("v", 1));
		mock.advance_millis(5);
		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("v", 2));

		registry.flush();
		assert!(
			push_rx.try_recv().is_err(),
			"the window anchors at the first pending change and is not reset by a later one"
		);

		mock.advance_millis(5);
		registry.flush();
		let entries = entries_of(push_rx.try_recv().expect("envelope after the window elapses"));
		assert_eq!(entries.len(), 1);
		let frames = entries[0].body.get("frames").expect("frames key").as_array().unwrap();
		assert_eq!(
			frames.len(),
			2,
			"both changes inside the linger window are concatenated into one frame-list"
		);
	}

	#[tokio::test]
	async fn test_linger_zero_flushes_on_the_next_flush() {
		let (_, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub_z = SubscriptionId(9);
		subscribe_shaped(&registry, sub_z, connection_id, &sink, 0, 0);

		registry.try_deliver(&sub_z, DiffType::Insert, single_int_columns("v", 1));
		registry.flush();
		let entries = entries_of(
			push_rx.try_recv()
				.expect("a zero-linger subscription is due immediately, with no added latency"),
		);
		assert_eq!(entries.len(), 1);
	}

	#[tokio::test]
	async fn test_single_subscription_honors_linger() {
		// A standalone (non-batch) subscription must hold the change for the linger window rather
		// than deliver immediately.
		let (mock, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub = SubscriptionId(21);
		registry.subscribe(
			sub,
			connection_id,
			sink,
			WireFormat::Frames,
			None,
			Duration::zero(),
			Duration::from_milliseconds(10).unwrap(),
		);

		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1));
		registry.flush();
		assert!(
			push_rx.try_recv().is_err(),
			"linger must hold the change on a single subscription, not deliver it immediately"
		);

		mock.advance_millis(10);
		registry.flush();
		assert!(push_rx.try_recv().is_ok(), "the held change is delivered once the linger window elapses");
	}

	#[tokio::test]
	async fn a_subscription_honors_throttle() {
		// A second change that lands inside the throttle interval must be held, not sent at once.
		let (mock, registry, connection_id, sink, mut push_rx) = registry_with_sink();

		let sub = SubscriptionId(22);
		subscribe_shaped(&registry, sub, connection_id, &sink, 10, 0);

		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1));
		registry.flush();
		assert!(push_rx.try_recv().is_ok(), "the first change flushes immediately (throttle not yet fired)");

		mock.advance_millis(5);
		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 2));
		registry.flush();
		assert!(
			push_rx.try_recv().is_err(),
			"throttle must hold a second change that lands inside its interval"
		);

		mock.advance_millis(5);
		registry.flush();
		assert!(push_rx.try_recv().is_ok(), "the subscription flushes once the throttle interval elapses");
	}

	#[tokio::test]
	async fn test_linger_next_deadline_tracks_the_nearest_subscription() {
		let (mock, registry, connection_id, sink, _push_rx) = registry_with_sink();

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);
		subscribe_shaped(&registry, sub_a, connection_id, &sink, 0, 5);
		subscribe_shaped(&registry, sub_b, connection_id, &sink, 0, 50);

		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("v", 1));
		registry.try_deliver(&sub_b, DiffType::Insert, single_int_columns("v", 2));

		assert_eq!(
			registry.flush(),
			Some(Duration::from_milliseconds(5).unwrap()),
			"the poller is told to wake at the soonest subscription deadline, so a low-linger subscription never starves"
		);

		mock.advance_millis(5);
		assert_eq!(
			registry.flush(),
			Some(Duration::from_milliseconds(45).unwrap()),
			"after the near subscription drains, the deadline tracks the remaining subscription"
		);
	}

	#[tokio::test]
	async fn an_unsubscribed_subscription_sends_none_of_its_queued_changes() {
		// A change queued before the unsubscribe must never reach the client after it was acknowledged.
		let (_, registry, connection_id, sink, mut push_rx) = registry_with_sink();
		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);
		subscribe_shaped(&registry, sub_a, connection_id, &sink, 0, 0);
		subscribe_shaped(&registry, sub_b, connection_id, &sink, 0, 0);
		registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("value", 1));
		registry.try_deliver(&sub_b, DiffType::Insert, single_int_columns("value", 2));

		registry.unsubscribe(sub_a);
		registry.flush();

		let entries =
			entries_of(push_rx.try_recv().expect("the remaining subscription's change must still be sent"));
		let ids: Vec<String> = entries.iter().map(|entry| entry.subscription_id.clone()).collect();
		assert_eq!(ids, vec![sub_b.to_string()]);
	}
}
