// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::future::Future;

use reifydb_client::{HttpClient, QueueClaimRequest, WireFormat, value::error::Error};
use rustls::crypto::ring::default_provider;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	spawn,
};

const CUT_SHORT_OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\ncut short";

const CUT_SHORT_ERROR: &str = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 1000\r\n\r\ncut short";

const NOT_JSON_OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nnot json";

async fn closed_port_client(format: WireFormat) -> HttpClient {
	let _ = default_provider().install_default();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	drop(listener);
	HttpClient::connect(&format!("http://{address}"), format).await.unwrap()
}

async fn client_answered_with(reply: &'static str, format: WireFormat) -> HttpClient {
	let _ = default_provider().install_default();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	spawn(async move {
		let (mut stream, _) = listener.accept().await.unwrap();
		read_request(&mut stream).await;
		stream.write_all(reply.as_bytes()).await.unwrap();
		stream.shutdown().await.unwrap();
	});
	HttpClient::connect(&format!("http://{address}"), format).await.unwrap()
}

async fn read_request(stream: &mut TcpStream) {
	// Replying before the request is fully read makes the close a reset, which hides the reply under test.
	let mut request = Vec::new();
	let mut chunk = [0u8; 4096];
	loop {
		let read = stream.read(&mut chunk).await.unwrap();
		assert!(read > 0, "the client closed before sending a full request");
		request.extend_from_slice(&chunk[..read]);
		let text = String::from_utf8_lossy(&request).to_lowercase();
		if let Some(head_end) = text.find("\r\n\r\n") {
			let body_len = text[..head_end]
				.lines()
				.find_map(|line| line.strip_prefix("content-length:"))
				.map_or(0, |value| value.trim().parse::<usize>().unwrap());
			if request.len() >= head_end + 4 + body_len {
				return;
			}
		}
	}
}

async fn error_code<T: Send + 'static>(call: impl Future<Output = Result<T, Error>> + Send + 'static) -> String {
	// A panic surfaces as a join error here, so only a returned error yields a code.
	match spawn(call).await {
		Ok(Err(error)) => error.0.code,
		Ok(Ok(_)) => panic!("expected the request to fail, it succeeded"),
		Err(join) => panic!("expected the request to fail, it panicked: {join}"),
	}
}

fn claim() -> QueueClaimRequest {
	QueueClaimRequest {
		queue: "test::q".to_string(),
		worker: "w".to_string(),
		..Default::default()
	}
}

#[tokio::test]
async fn an_http_login_to_a_closed_port_is_a_transport_error_not_a_panic() {
	// A login that cannot reach the server is the caller's to retry, so it must come back as an error.
	let mut client = closed_port_client(WireFormat::Frames).await;

	let code = error_code(async move { client.login_with_token("token").await.map(|_| ()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_login_reply_cut_short_is_a_transport_error_not_a_panic() {
	// A body that ends early must fail the login, not abort the task reading it.
	let mut client = client_answered_with(CUT_SHORT_OK, WireFormat::Frames).await;

	let code = error_code(async move { client.login_with_token("token").await.map(|_| ()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_login_reply_that_is_not_json_is_a_decode_error_not_a_panic() {
	// A proxy page or garbage body must fail the login as undecodable, never panic the caller.
	let mut client = client_answered_with(NOT_JSON_OK, WireFormat::Frames).await;

	let code = error_code(async move { client.login_with_token("token").await.map(|_| ()) }).await;

	assert_eq!(code, "DECODE");
}

#[tokio::test]
async fn an_http_logout_to_a_closed_port_is_a_transport_error_not_a_panic() {
	// A logout that never reached the server must say so, or the caller believes the token was revoked.
	let mut client = closed_port_client(WireFormat::Frames).await;
	client.authenticate("token");

	let code = error_code(async move { client.logout().await }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_logout_error_reply_cut_short_is_a_transport_error_not_a_panic() {
	// A failed logout whose error body ends early must still fail, not abort the task reading it.
	let mut client = client_answered_with(CUT_SHORT_ERROR, WireFormat::Frames).await;
	client.authenticate("token");

	let code = error_code(async move { client.logout().await }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_frames_query_reply_cut_short_is_a_transport_error_not_a_panic() {
	// A frames body that ends early must fail the query, never read as a shorter answer or a panic.
	let client = client_answered_with(CUT_SHORT_OK, WireFormat::Frames).await;

	let code = error_code(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_rbcf_query_to_a_closed_port_is_a_transport_error_not_a_panic() {
	// The rbcf path sends on its own, so an unreachable server must fail it the same way as frames.
	let client = closed_port_client(WireFormat::Rbcf).await;

	let code = error_code(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_rbcf_query_reply_cut_short_is_a_transport_error_not_a_panic() {
	// An rbcf body that ends early must fail the query, not abort the task reading its bytes.
	let client = client_answered_with(CUT_SHORT_OK, WireFormat::Rbcf).await;

	let code = error_code(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_rbcf_query_error_reply_cut_short_is_a_transport_error_not_a_panic() {
	// An error body that ends early must still fail the query, not abort the task reading it.
	let client = client_answered_with(CUT_SHORT_ERROR, WireFormat::Rbcf).await;

	let code = error_code(async move { client.query("from test::t", None).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_queue_claim_to_a_closed_port_is_a_transport_error_not_a_panic() {
	// A worker polling an unreachable server must get an error it can back off on, not a dead task.
	let client = closed_port_client(WireFormat::Rbcf).await;

	let code = error_code(async move { client.queue_claim(claim()).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_queue_claim_reply_cut_short_is_a_transport_error_not_a_panic() {
	// A claim body that ends early must fail, or the worker task dies holding leased messages.
	let client = client_answered_with(CUT_SHORT_OK, WireFormat::Rbcf).await;

	let code = error_code(async move { client.queue_claim(claim()).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}

#[tokio::test]
async fn an_http_queue_claim_error_reply_cut_short_is_a_transport_error_not_a_panic() {
	// A claim error body that ends early must still fail, not abort the worker task.
	let client = client_answered_with(CUT_SHORT_ERROR, WireFormat::Rbcf).await;

	let code = error_code(async move { client.queue_claim(claim()).await.map(|frames| frames.len()) }).await;

	assert_eq!(code, "TRANSPORT");
}
