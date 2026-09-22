// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fs,
	future::poll_fn,
	path::{Path, PathBuf},
	thread,
};

use futures_util::{SinkExt, StreamExt};
use reifydb::{Database, server, sub::subsystem::Subsystem};
use reifydb_console_protocol::{PROTOCOL_VERSION, Refusal, Register, Reply, read_message, write_message};
use reifydb_sub_console::{config::ExternalAccess, subsystem::ConsoleSubsystem};
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::{params::Params, value::duration::Duration};
use serde_json::{Value, from_str};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	runtime::Runtime,
	sync::mpsc,
	time::timeout,
};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode};

const AUTH: &str = r#"{"id":"1","type":"Auth","payload":{"token":"t"}}"#;
const QUERY: &str = r#"{"id":"2","type":"Query","payload":{"rql":"FROM demo::note MAP { id, body }"}}"#;
const PROJECT_URL: &str = "ws://localhost/v1/project/p-1";
const TOKEN: &str = "t-1";

struct FakeTunnel {
	address: String,
	arrivals: mpsc::UnboundedReceiver<TcpStream>,
}

impl FakeTunnel {
	async fn start() -> Self {
		// Every accepted connection must reach the test, otherwise the dial count is wrong.
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake tunnel server");
		let address = listener.local_addr().expect("fake tunnel address").to_string();
		let (arrived, arrivals) = mpsc::unbounded_channel();
		tokio::spawn(async move {
			loop {
				let (tcp, _) = listener.accept().await.expect("fake tunnel accept");
				if arrived.send(tcp).is_err() {
					return;
				}
			}
		});
		Self {
			address,
			arrivals,
		}
	}

	async fn next_register(&mut self) -> (TcpStream, Register) {
		let mut tcp = timeout(seconds(10).to_std(), self.arrivals.recv())
			.await
			.expect("the instance dials within 10 s")
			.expect("fake tunnel still accepting");
		let register = read_message::<_, Register>(&mut tcp).await.expect("a Register message");
		(tcp, register)
	}
}

fn seconds(n: i64) -> Duration {
	Duration::from_seconds(n).expect("seconds")
}

fn database(address: &str, fingerprint_path: Option<&Path>) -> Database {
	// Admin, so tests about dialing and serving are not closed by the level, which has its own tests.
	database_at(address, fingerprint_path, ExternalAccess::Admin)
}

fn database_at(address: &str, fingerprint_path: Option<&Path>, access: ExternalAccess) -> Database {
	let address = address.to_string();
	let path = fingerprint_path.map(Path::to_path_buf);
	server::memory()
		.with_ws(|ws| ws)
		.with_console(move |console| {
			let console = console.address(address).token(TOKEN).external_access(access);
			match path {
				Some(path) => console.fingerprint_path(path),
				None => console,
			}
		})
		.build()
		.expect("database with console builds")
}

fn seed(db: &Database) {
	db.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 't' }", Params::None)
		.expect("create root token");
	db.admin_as_root("CREATE NAMESPACE demo", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE demo::note { id: int4, body: utf8 }", Params::None).expect("create table");
	db.command_as_root("INSERT demo::note [{ id: 1, body: 'first' }]", Params::None).expect("insert first row");
}

fn console(db: &Database) -> &ConsoleSubsystem {
	db.subsystem::<ConsoleSubsystem>().expect("console subsystem")
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
	// Polls, so a slow machine costs time and never a false failure.
	for _ in 0..100 {
		if done() {
			return;
		}
		thread::sleep(Duration::from_milliseconds(100).expect("100 ms").to_std());
	}
	panic!("{what} did not happen within 10 s");
}

fn registered(fingerprint: &str) -> Reply {
	Reply::Registered {
		fingerprint: fingerprint.to_string(),
		protocol_version: PROTOCOL_VERSION,
	}
}

async fn reply(tcp: &mut TcpStream, reply: Reply) {
	write_message(tcp, &reply).await.expect("send reply");
}

async fn open_stream(tcp: TcpStream) -> Compat<yamux::Stream> {
	// The client side must keep polling the connection, otherwise no frame of the opened stream moves.
	let mut connection = Connection::new(tcp.compat(), Config::default(), Mode::Client);
	let stream = poll_fn(|cx| connection.poll_new_outbound(cx)).await.expect("open a yamux stream");
	tokio::spawn(async move { while let Some(Ok(_)) = poll_fn(|cx| connection.poll_next_inbound(cx)).await {} });
	stream.compat()
}

async fn request(ws: &mut WebSocketStream<Compat<yamux::Stream>>, body: &str) -> Value {
	ws.send(Message::Text(body.into())).await.expect("send request");
	loop {
		let message = timeout(seconds(10).to_std(), ws.next())
			.await
			.expect("a message within 10 s")
			.expect("stream still open")
			.expect("a websocket frame");
		if let Message::Text(text) = message {
			return from_str(&text).expect("a json message");
		}
	}
}

#[test]
fn registers_and_serves_a_stream() {
	// A stream the tunnel server opens must reach the instance's own WS server: upgrade, Auth, Query.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database(&fake.address, None);
	seed(&db);
	runtime.block_on(async {
		let (mut tcp, register) = fake.next_register().await;
		assert_eq!(register.protocol_version, PROTOCOL_VERSION);
		assert_eq!(register.token, TOKEN);
		assert_eq!(register.fingerprint, None, "no path means a new instance");
		assert_eq!(register.version, env!("CARGO_PKG_VERSION"));
		reply(&mut tcp, registered("fp-1")).await;

		let stream = open_stream(tcp).await;
		let (mut ws, _) = client_async(PROJECT_URL, stream).await.expect("upgrade through the tunnel");
		let auth = request(&mut ws, AUTH).await;
		assert_eq!(auth["type"], "Auth", "{auth}");
		let query = request(&mut ws, QUERY).await;
		assert_eq!(query["type"], "Query", "{query}");
		assert!(query["payload"]["body"].to_string().contains("first"), "{query}");
	});
}

#[test]
fn the_fingerprint_is_saved_and_presented_again() {
	// A restarted instance must come back as the same instance, so the minted fingerprint must survive on disk.
	temp_dir(|dir| {
		let path: PathBuf = dir.join("state").join("fingerprint");
		let runtime = Runtime::new().expect("test runtime");
		let mut fake = runtime.block_on(FakeTunnel::start());

		let first = database(&fake.address, Some(&path));
		let held = runtime.block_on(async {
			let (mut tcp, register) = fake.next_register().await;
			assert_eq!(register.fingerprint, None, "nothing saved yet");
			reply(&mut tcp, registered("fp-1")).await;
			tcp
		});
		wait_for("the fingerprint file", || fs::read_to_string(&path).is_ok_and(|saved| !saved.is_empty()));
		assert_eq!(fs::read_to_string(&path).expect("read fingerprint"), "fp-1");
		drop(first);
		drop(held);

		let second = database(&fake.address, Some(&path));
		runtime.block_on(async {
			let (_tcp, register) = fake.next_register().await;
			assert_eq!(register.fingerprint.as_deref(), Some("fp-1"));
		});
		drop(second);
		Ok(())
	})
	.expect("temp dir");
}

#[test]
fn a_final_refusal_stops_dialing() {
	// A final refusal must stop the session for good, otherwise a revoked token hammers the tunnel server.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database(&fake.address, None);
	runtime.block_on(async {
		let (mut tcp, _) = fake.next_register().await;
		reply(
			&mut tcp,
			Reply::Refused {
				reason: Refusal::UnknownToken,
			},
		)
		.await;
	});
	wait_for("the console session to stop", || !console(&db).is_running());
	assert!(console(&db).health_status().is_failed());
	// Must outlast the first two backoff waits (500 ms, 1 s), otherwise a redial goes unseen.
	let redial = runtime.block_on(async { timeout(seconds(2).to_std(), fake.arrivals.recv()).await });
	assert!(redial.is_err(), "a final refusal must never dial again");
}

#[test]
fn an_unavailable_refusal_dials_again() {
	// Unavailable is the one retryable refusal, so a busy control plane must never park the instance.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database(&fake.address, None);
	runtime.block_on(async {
		let (mut tcp, _) = fake.next_register().await;
		reply(
			&mut tcp,
			Reply::Refused {
				reason: Refusal::Unavailable,
			},
		)
		.await;
		let (_tcp, again) = fake.next_register().await;
		assert_eq!(again.token, TOKEN);
	});
	assert!(console(&db).is_running());
}

#[test]
fn a_lost_session_dials_again() {
	// A dropped tunnel must be redialed, presenting the fingerprint it was given even without a path.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database(&fake.address, None);
	runtime.block_on(async {
		let (mut tcp, first) = fake.next_register().await;
		assert_eq!(first.fingerprint, None);
		reply(&mut tcp, registered("fp-1")).await;
		// Half close and drain, so the drop is a clean FIN and never a reset that could eat the reply.
		tcp.shutdown().await.expect("half close");
		let mut rest = Vec::new();
		tcp.read_to_end(&mut rest).await.expect("instance closes its end");
		drop(tcp);

		let (_tcp, second) = fake.next_register().await;
		assert_eq!(second.fingerprint.as_deref(), Some("fp-1"));
	});
	assert!(console(&db).is_running());
}

#[test]
fn with_console_without_ws_fails_to_build() {
	// The console hands streams to the WS acceptor, so building without it must fail, not dial into nothing.
	let result = server::memory().with_console(|console| console.address("127.0.0.1:1").token(TOKEN)).build();
	let error = result.err().expect("build without with_ws must fail");
	assert!(error.to_string().contains("with_ws"), "{error}");
}

#[test]
fn with_console_before_with_ws_still_builds() {
	// The builder must order the console after WS itself, otherwise call order decides whether it builds.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let address = fake.address.clone();
	let _db = server::memory()
		.with_console(move |console| console.address(address).token(TOKEN))
		.with_ws(|ws| ws)
		.build()
		.expect("console before ws builds");
	let (_tcp, register) = runtime.block_on(fake.next_register());
	assert_eq!(register.token, TOKEN);
}

#[test]
fn with_console_without_an_address_fails_to_build() {
	// A console with nowhere to dial must fail at build time, never at the first dial.
	let result = server::memory().with_ws(|ws| ws).with_console(|console| console.token(TOKEN)).build();
	let error = result.err().expect("build without an address must fail");
	assert!(error.to_string().contains("address"), "{error}");
}

#[test]
fn with_console_without_a_token_fails_to_build() {
	// A console without a token can never register, so it must fail at build time.
	let result = server::memory().with_ws(|ws| ws).with_console(|console| console.address("127.0.0.1:1")).build();
	let error = result.err().expect("build without a token must fail");
	assert!(error.to_string().contains("token"), "{error}");
}

const COMMAND: &str =
	r#"{"id":"3","type":"Command","payload":{"rql":"INSERT demo::note [{ id: 2, body: 'second' }]"}}"#;
const ADMIN: &str = r#"{"id":"4","type":"Admin","payload":{"rql":"CREATE NAMESPACE other"}}"#;
const CALL: &str = r#"{"id":"5","type":"Call","payload":{"name":"demo::anything"}}"#;
const CLAIM: &str = r#"{"id":"6","type":"QueueClaim","payload":{"queue":"demo::jobs","worker":"w"}}"#;

fn code(response: &Value) -> &str {
	response["payload"]["diagnostic"]["code"].as_str().unwrap_or("")
}

async fn tunneled(fake: &mut FakeTunnel, access: ExternalAccess) -> WebSocketStream<Compat<yamux::Stream>> {
	let (mut tcp, register) = fake.next_register().await;
	assert_eq!(register.external_access, access, "the instance must announce the level it was built with");
	reply(&mut tcp, registered("fp-1")).await;
	let stream = open_stream(tcp).await;
	let (mut ws, _) = client_async(PROJECT_URL, stream).await.expect("upgrade through the tunnel");
	let auth = request(&mut ws, AUTH).await;
	assert_eq!(auth["type"], "Auth", "{auth}");
	ws
}

#[test]
fn a_query_level_stream_reads_and_refuses_every_write() {
	// The tunnel server cannot see requests, so the instance is the only place a write through the console is stopped.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database_at(&fake.address, None, ExternalAccess::Query);
	seed(&db);
	runtime.block_on(async {
		let mut ws = tunneled(&mut fake, ExternalAccess::Query).await;

		let query = request(&mut ws, QUERY).await;
		assert_eq!(query["type"], "Query", "{query}");

		for write in [COMMAND, CALL, CLAIM, ADMIN] {
			let refused = request(&mut ws, write).await;
			assert_eq!(code(&refused), "FORBIDDEN", "{write} must be refused at query level: {refused}");
		}

		let after = request(&mut ws, QUERY).await;
		assert!(!after["payload"]["body"].to_string().contains("second"), "a refused write must not land: {after}");
	});
}

#[test]
fn a_command_level_stream_writes_and_refuses_admin() {
	// Command must not quietly include admin, otherwise the level below admin could still change the schema.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database_at(&fake.address, None, ExternalAccess::Command);
	seed(&db);
	runtime.block_on(async {
		let mut ws = tunneled(&mut fake, ExternalAccess::Command).await;

		let written = request(&mut ws, COMMAND).await;
		assert_eq!(written["type"], "Command", "{written}");
		let after = request(&mut ws, QUERY).await;
		assert!(after["payload"]["body"].to_string().contains("second"), "the write must land: {after}");

		let refused = request(&mut ws, ADMIN).await;
		assert_eq!(code(&refused), "FORBIDDEN", "admin must be refused at command level: {refused}");
	});
}

#[test]
fn an_admin_level_stream_is_not_refused_by_the_level() {
	// Admin is the top level, so nothing it sends may be refused for the level itself.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database_at(&fake.address, None, ExternalAccess::Admin);
	seed(&db);
	runtime.block_on(async {
		let mut ws = tunneled(&mut fake, ExternalAccess::Admin).await;

		for sent in [COMMAND, CALL, CLAIM, ADMIN] {
			let answered = request(&mut ws, sent).await;
			assert_ne!(code(&answered), "FORBIDDEN", "{sent} must pass the level check at admin: {answered}");
		}
	});
}

#[test]
fn an_off_instance_registers_but_serves_no_stream() {
	// Off must hold on the instance too, otherwise a stale tunnel gate would reach the database.
	let runtime = Runtime::new().expect("test runtime");
	let mut fake = runtime.block_on(FakeTunnel::start());
	let db = database_at(&fake.address, None, ExternalAccess::Off);
	seed(&db);
	runtime.block_on(async {
		let (mut tcp, register) = fake.next_register().await;
		assert_eq!(register.external_access, ExternalAccess::Off);
		reply(&mut tcp, registered("fp-1")).await;

		let stream = open_stream(tcp).await;
		let upgraded = timeout(seconds(10).to_std(), client_async(PROJECT_URL, stream))
			.await
			.expect("the dropped stream must end the upgrade within 10 s");
		assert!(upgraded.is_err(), "an off instance must never serve a websocket");
	});
}
