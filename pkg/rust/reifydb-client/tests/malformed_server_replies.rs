// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use futures_util::{SinkExt, StreamExt};
use reifydb_client::{HttpClient, SubscribeItem, SubscriptionConfig, WireFormat, ws::WsClient};
use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::value::{batch::batch, column::factory};
use reifydb_value::value::{duration::Duration, frame::frame::Frame};
use rustls::crypto::ring::default_provider;
use serde_json::{Value, from_str, json};
use tokio::{net::TcpListener, spawn, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const WAIT: Duration = Duration::from_seconds_const(5);

async fn ws_server_sending(replies: impl FnOnce(&str) -> Vec<Message> + Send + 'static) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	spawn(async move {
		let (stream, _) = listener.accept().await.unwrap();
		let mut socket = accept_async(stream).await.unwrap();
		let request = loop {
			match socket.next().await {
				Some(Ok(Message::Text(text))) => break from_str::<Value>(&text).unwrap(),
				Some(Ok(_)) => continue,
				_ => return,
			}
		};
		for reply in replies(request["id"].as_str().unwrap()) {
			socket.send(reply).await.unwrap();
		}
		while socket.next().await.is_some() {}
	});
	format!("ws://{address}")
}

async fn ws_server_replying(reply: impl FnOnce(&str) -> Message + Send + 'static) -> String {
	ws_server_sending(|id| vec![reply(id)]).await
}

fn one_row_rbcf() -> Vec<u8> {
	let frame = Frame::from(batch(vec![factory::int4("a", [7])]).expect("an int4 column forms a batch"));
	encode_frames(&[frame], &EncodeOptions::default()).unwrap()
}

fn query_envelope(id: &str, meta: &str) -> Vec<u8> {
	let mut envelope = vec![0u8];
	envelope.extend_from_slice(&(id.len() as u32).to_le_bytes());
	envelope.extend_from_slice(id.as_bytes());
	envelope.extend_from_slice(&(meta.len() as u32).to_le_bytes());
	envelope.extend_from_slice(meta.as_bytes());
	envelope.extend_from_slice(&one_row_rbcf());
	envelope
}

fn binary_query_reply(id: &str, meta: &str) -> Message {
	Message::binary(query_envelope(id, meta))
}

fn change_envelope(subscription_id: &str, rbcf: &[u8]) -> Vec<u8> {
	let mut envelope = vec![1u8];
	envelope.extend_from_slice(&1u32.to_le_bytes());
	envelope.extend_from_slice(&(subscription_id.len() as u32).to_le_bytes());
	envelope.extend_from_slice(subscription_id.as_bytes());
	envelope.extend_from_slice(&(rbcf.len() as u32).to_le_bytes());
	envelope.extend_from_slice(rbcf);
	envelope
}

#[tokio::test]
async fn a_ws_text_reply_the_client_cannot_parse_fails_the_request_instead_of_hanging() {
	// A reply that parses as neither a response nor a push must fail the request, or the caller waits forever.
	let url = ws_server_replying(|id| {
		Message::text(json!({ "id": id, "type": "Query", "payload": "not a query response" }).to_string())
	})
	.await;
	let client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result = timeout(WAIT.to_std(), client.query("from test::t", None)).await;

	assert!(matches!(result, Ok(Err(_))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_binary_reply_with_malformed_meta_fails_the_request_instead_of_dropping_the_meta() {
	// Meta that fails to parse must fail the reply, or a broken reply looks like one that carried no meta.
	let url = ws_server_replying(|id| binary_query_reply(id, "not json")).await;
	let client = WsClient::connect(&url, WireFormat::Rbcf).await.unwrap();

	let result = timeout(WAIT.to_std(), client.query_with_meta("from test::t", None))
		.await
		.map(|reply| reply.map(|r| r.meta));

	assert!(matches!(result, Ok(Err(_))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_binary_reply_to_a_json_only_request_fails_the_request_instead_of_panicking() {
	// A server that answers an auth request in binary must fail that request, not panic the caller.
	let url = ws_server_replying(|id| binary_query_reply(id, "")).await;
	let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result =
		spawn(
			async move { timeout(WAIT.to_std(), client.authenticate("token")).await.map(|auth| auth.is_err()) },
		)
		.await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_binary_reply_cut_short_inside_its_meta_fails_the_request_instead_of_hanging() {
	// The request id is intact, so a reply truncated after it must fail that request or the caller waits forever.
	let meta = r#"{"fingerprint":"0123456789abcdef","duration":"1ms"}"#;
	let url = ws_server_replying(move |id| {
		let mut envelope = query_envelope(id, meta);
		envelope.truncate(1 + 4 + id.len() + 4 + meta.len() / 2);
		Message::binary(envelope)
	})
	.await;
	let client = WsClient::connect(&url, WireFormat::Rbcf).await.unwrap();

	let result = timeout(WAIT.to_std(), client.query("from test::t", None)).await;

	assert!(matches!(result, Ok(Err(_))), "expected the request to fail, got {result:?}");
}

fn subscribed(id: &str, server_ids: &[&str]) -> Message {
	let subscriptions: Vec<Value> = server_ids
		.iter()
		.enumerate()
		.map(|(index, server_id)| json!({ "index": index, "subscription_id": server_id }))
		.collect();
	Message::text(
		json!({ "id": id, "type": "Subscribed", "payload": { "subscriptions": subscriptions } }).to_string(),
	)
}

fn items() -> Vec<SubscribeItem<'static>> {
	vec![
		SubscribeItem::new("from test::a", SubscriptionConfig::default()),
		SubscribeItem::new("from test::b", SubscriptionConfig::default()),
	]
}

#[tokio::test]
async fn a_ws_change_cut_short_inside_an_entry_reaches_the_subscriber_as_a_decode_error() {
	// The subscription id is intact, so a truncated entry must surface as a decode error, not vanish.
	let url = ws_server_sending(|id| {
		let mut envelope = change_envelope("s1", &one_row_rbcf());
		envelope.truncate(envelope.len() - 8);
		vec![subscribed(id, &["s1"]), Message::binary(envelope)]
	})
	.await;
	let client = WsClient::connect(&url, WireFormat::Rbcf).await.unwrap();
	let item = SubscribeItem::new("from test::t", SubscriptionConfig::default());
	let mut handles = client.subscribe(&[item]).await.unwrap();

	let event = timeout(WAIT.to_std(), handles[0].recv()).await;

	assert!(
		matches!(&event, Ok(Some(change)) if change.decode_error.is_some()),
		"expected a decode error for the truncated entry, got {event:?}"
	);
}

#[tokio::test]
async fn a_closed_subscription_ends_only_its_own_handle() {
	// Closing every handle on one close, or ignoring the close, leaves a caller waiting forever or blind.
	let url = ws_server_sending(|id| {
		let change = json!({ "type": "Change", "payload": { "entries": [
			{ "subscription_id": "s2", "content_type": "application/vnd.reifydb.frames", "body": { "frames": [] } }
		] } });
		vec![
			subscribed(id, &["s1", "s2"]),
			Message::text(
				json!({ "type": "SubscriptionClosed", "payload": { "subscription_id": "s1" } })
					.to_string(),
			),
			Message::text(change.to_string()),
		]
	})
	.await;
	let client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
	let mut handles = client.subscribe(&items()).await.unwrap();

	let first = timeout(WAIT.to_std(), handles[0].recv()).await;
	let second = timeout(WAIT.to_std(), handles[1].recv()).await;

	assert!(matches!(first, Ok(None)), "the closed handle must end, got {first:?}");
	assert!(matches!(second, Ok(Some(_))), "its sibling must keep receiving, got {second:?}");
}

#[tokio::test]
async fn a_subscribed_ack_missing_an_item_fails_the_call() {
	// Accepting a short ack hands the caller a handle no server subscription stands behind.
	let url = ws_server_sending(|id| vec![subscribed(id, &["s1"])]).await;
	let client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result = timeout(WAIT.to_std(), client.subscribe(&items())).await;

	assert!(matches!(result, Ok(Err(_))), "a short ack must fail the call");
}

#[tokio::test]
async fn an_http_request_to_a_closed_port_is_an_error_not_a_panic() {
	// A transport failure is the caller's to handle, so it must come back as an error, not abort the task.
	let _ = default_provider().install_default();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	drop(listener);
	let client = HttpClient::connect(&format!("http://{address}"), WireFormat::Frames).await.unwrap();

	let result = spawn(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert!(matches!(result, Ok(Err(_))), "expected the request to fail, got {result:?}");
}
