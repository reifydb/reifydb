// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use futures_util::{SinkExt, StreamExt};
use reifydb::{
	Database, WithSubsystem,
	runtime::shutdown::Shutdown,
	server,
	sub_server_ws::acceptor::{Access, WsStreamAcceptor},
};
use reifydb_value::{params::Params, value::duration::Duration};
use serde_json::{Value, from_str};
use tokio::{
	io::{AsyncRead, AsyncWrite, DuplexStream, duplex},
	net::TcpStream,
	runtime::Runtime,
	time::timeout,
};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

const AUTH: &str = r#"{"id":"1","type":"Auth","payload":{"token":"t"}}"#;
const QUERY: &str = r#"{"id":"2","type":"Query","payload":{"rql":"FROM demo::note MAP { id, body }"}}"#;
const SUBSCRIBE: &str = r#"{"id":"3","type":"Subscribe","payload":{"rql":"FROM demo::note"}}"#;
const PROJECT_URL: &str = "ws://localhost/v1/project/p-1";

fn database(max_connections: usize) -> Database {
	let db = server::memory()
		.with_flow(|flow| flow)
		.with_ws(move |ws| ws.bind_addr("127.0.0.1:0").max_connections(max_connections))
		.build()
		.expect("database with ws builds");
	db.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 't' }", Params::None)
		.expect("create root token");
	db.admin_as_root("CREATE NAMESPACE demo", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE demo::note { id: int4, body: utf8 }", Params::None).expect("create table");
	db.command_as_root("INSERT demo::note [{ id: 1, body: 'first' }]", Params::None).expect("insert first row");
	db
}

async fn over_duplex(acceptor: &WsStreamAcceptor) -> WebSocketStream<DuplexStream> {
	let (client, server) = duplex(64 * 1024);
	acceptor.accept(server, None, Access::Admin);
	let (ws, _) = client_async(PROJECT_URL, client).await.expect("upgrade over duplex");
	ws
}

async fn over_tcp(port: u16) -> WebSocketStream<TcpStream> {
	let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp connect");
	let (ws, _) = client_async(format!("ws://127.0.0.1:{port}/"), tcp).await.expect("upgrade over tcp");
	ws
}

async fn refused_upgrade(acceptor: &WsStreamAcceptor) -> bool {
	let (client, server) = duplex(64 * 1024);
	acceptor.accept(server, None, Access::Admin);
	timeout(Duration::from_seconds(10).expect("ten seconds").to_std(), client_async(PROJECT_URL, client))
		.await
		.expect("a dropped stream must fail the upgrade, not hang it")
		.is_err()
}

async fn request<S>(ws: &mut WebSocketStream<S>, body: &str) -> Value
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	ws.send(Message::Text(body.into())).await.expect("send request");
	next_json(ws).await
}

async fn next_json<S>(ws: &mut WebSocketStream<S>) -> Value
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	loop {
		let message = timeout(Duration::from_seconds(10).expect("ten seconds").to_std(), ws.next())
			.await
			.expect("a message within 10 s")
			.expect("stream still open")
			.expect("a websocket frame");
		if let Message::Text(text) = message {
			return from_str(&text).expect("a json message");
		}
	}
}

fn without_duration(mut response: Value) -> Value {
	let Some(meta) = response.pointer_mut("/payload/meta").and_then(Value::as_object_mut) else {
		panic!("query response without meta: {response}");
	};
	meta.remove("duration");
	response
}

#[test]
fn a_duplex_stream_answers_like_a_tcp_client() {
	// Auth and Query over a handed-in stream must match a TCP client byte for byte, timing aside.
	let db = database(10);
	let ws = db.sub_server_ws().expect("ws subsystem");
	let acceptor = ws.acceptor();
	let port = ws.port().expect("bound port");
	let runtime = Runtime::new().expect("test runtime");
	runtime.block_on(async {
		let mut via_duplex = over_duplex(&acceptor).await;
		let mut via_tcp = over_tcp(port).await;

		let auth_duplex = request(&mut via_duplex, AUTH).await;
		let auth_tcp = request(&mut via_tcp, AUTH).await;
		assert_eq!(auth_duplex["type"], "Auth", "{auth_duplex}");
		assert_eq!(auth_duplex, auth_tcp);

		let query_duplex = request(&mut via_duplex, QUERY).await;
		let query_tcp = request(&mut via_tcp, QUERY).await;
		assert_eq!(query_duplex["type"], "Query", "{query_duplex}");
		assert!(query_duplex["payload"]["body"].to_string().contains("first"), "{query_duplex}");
		assert_eq!(without_duration(query_duplex), without_duration(query_tcp));
	});
}

#[test]
fn a_duplex_stream_receives_subscription_changes() {
	// A subscription opened over a handed-in stream must push a row inserted afterwards.
	let db = database(10);
	let acceptor = db.sub_server_ws().expect("ws subsystem").acceptor();
	let runtime = Runtime::new().expect("test runtime");
	let (mut via_duplex, subscription_id) = runtime.block_on(async {
		let mut via_duplex = over_duplex(&acceptor).await;
		let auth = request(&mut via_duplex, AUTH).await;
		assert_eq!(auth["type"], "Auth", "{auth}");
		let subscribed = request(&mut via_duplex, SUBSCRIBE).await;
		assert_eq!(subscribed["type"], "Subscribed", "{subscribed}");
		let subscription_id = subscribed["payload"]["subscription_id"].clone();
		(via_duplex, subscription_id)
	});

	db.command_as_root("INSERT demo::note [{ id: 2, body: 'second' }]", Params::None).expect("insert second row");

	runtime.block_on(async {
		loop {
			let push = next_json(&mut via_duplex).await;
			if push["type"] == "Change" && push["payload"]["body"].to_string().contains("second") {
				assert_eq!(push["payload"]["subscription_id"], subscription_id, "{push}");
				break;
			}
		}
	});
}

#[test]
fn the_acceptor_is_registered_in_the_ioc_container() {
	// Other subsystems find the acceptor only through the container, so it must be there and serve.
	let db = database(10);
	let acceptor = db.engine().services().ioc.resolve::<WsStreamAcceptor>().expect("acceptor registered");
	let runtime = Runtime::new().expect("test runtime");
	runtime.block_on(async {
		let mut via_duplex = over_duplex(&acceptor).await;
		let auth = request(&mut via_duplex, AUTH).await;
		assert_eq!(auth["type"], "Auth", "{auth}");
	});
}

#[test]
fn a_stream_is_dropped_when_the_tcp_path_holds_the_last_slot() {
	// Both paths share one connection limit, so a full server must refuse the handed-in stream.
	let db = database(1);
	let ws = db.sub_server_ws().expect("ws subsystem");
	let acceptor = ws.acceptor();
	let port = ws.port().expect("bound port");
	let runtime = Runtime::new().expect("test runtime");
	runtime.block_on(async {
		let _via_tcp = over_tcp(port).await;
		assert!(refused_upgrade(&acceptor).await);
	});
}

#[test]
fn a_stream_is_dropped_after_ws_shutdown() {
	// A stopped ws subsystem must refuse new streams even while the runtime would still run them.
	let db = database(10);
	let ws = db.sub_server_ws().expect("ws subsystem");
	let acceptor = ws.acceptor();
	ws.shutdown();
	let runtime = Runtime::new().expect("test runtime");
	runtime.block_on(async {
		assert!(refused_upgrade(&acceptor).await);
	});
}
