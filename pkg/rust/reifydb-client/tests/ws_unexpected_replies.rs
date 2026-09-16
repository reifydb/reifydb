// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, time::Duration};

use futures_util::{SinkExt, StreamExt};
use reifydb_client::{WireFormat, ws::WsClient};
use serde_json::{Value, from_str, json};
use tokio::{net::TcpListener, spawn, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const WAIT: Duration = Duration::from_secs(5);

async fn ws_server_answering(reply: impl Fn(&str) -> Value + Send + 'static) -> String {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	spawn(async move {
		let (stream, _) = listener.accept().await.unwrap();
		let mut socket = accept_async(stream).await.unwrap();
		while let Some(message) = socket.next().await {
			let Ok(Message::Text(text)) = message else {
				continue;
			};
			let request = from_str::<Value>(&text).unwrap();
			let answer = reply(request["id"].as_str().unwrap());
			if socket.send(Message::text(answer.to_string())).await.is_err() {
				return;
			}
		}
	});
	format!("ws://{address}")
}

fn logout_reply(id: &str) -> Value {
	json!({ "id": id, "type": "Logout", "payload": { "status": "ok" } })
}

fn auth_reply(id: &str, status: &str) -> Value {
	json!({ "id": id, "type": "Auth", "payload": { "status": status, "token": "t", "identity": "i" } })
}

#[tokio::test]
async fn a_ws_auth_reply_of_the_wrong_type_fails_the_request_instead_of_panicking() {
	// A server answering auth with another reply type must fail that request, not panic the caller.
	let url = ws_server_answering(|id| logout_reply(id)).await;
	let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result =
		spawn(async move { timeout(WAIT, client.authenticate("token")).await.map(|auth| auth.is_err()) }).await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_login_the_server_does_not_authenticate_fails_the_request_instead_of_panicking() {
	// A login answered with any status but authenticated was refused, so the caller must get an error.
	let url = ws_server_answering(|id| auth_reply(id, "challenge")).await;
	let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result = spawn(async move {
		timeout(WAIT, client.login("password", HashMap::new())).await.map(|login| login.is_err())
	})
	.await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_login_reply_of_the_wrong_type_fails_the_request_instead_of_panicking() {
	// A server answering login with another reply type must fail that request, not panic the caller.
	let url = ws_server_answering(|id| logout_reply(id)).await;
	let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result = spawn(async move {
		timeout(WAIT, client.login("password", HashMap::new())).await.map(|login| login.is_err())
	})
	.await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_logout_reply_of_the_wrong_type_fails_the_request_instead_of_panicking() {
	// A server answering logout with another reply type must fail that request, not panic the caller.
	let url = ws_server_answering(|id| auth_reply(id, "authenticated")).await;
	let mut client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();
	client.authenticate("token").await.unwrap();

	let result = spawn(async move { timeout(WAIT, client.logout()).await.map(|logout| logout.is_err()) }).await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}

#[tokio::test]
async fn a_ws_unsubscribe_reply_of_the_wrong_type_fails_the_request_instead_of_panicking() {
	// A server answering unsubscribe with another reply type must fail that request, not panic the caller.
	let url = ws_server_answering(|id| logout_reply(id)).await;
	let client = WsClient::connect(&url, WireFormat::Frames).await.unwrap();

	let result =
		spawn(
			async move { timeout(WAIT, client.unsubscribe("1")).await.map(|unsubscribe| unsubscribe.is_err()) },
		)
		.await;

	assert!(matches!(result, Ok(Ok(true))), "expected the request to fail, got {result:?}");
}
