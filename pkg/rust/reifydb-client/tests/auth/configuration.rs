// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use bs58::encode as bs58_encode;
use ed25519_dalek::{Signer, SigningKey};
use reifydb::{
	Database, RuntimeConfig,
	runtime::context::clock::{Clock, MockClock},
	server,
};
use reifydb_client::{HttpClient, WireFormat, WsClient};
use reifydb_value::value::duration::Duration;
use serde_json::{Value as JsonValue, json};
use tokio::runtime::Runtime;

use crate::{
	auth::start_server_with_auth_users,
	common::{cleanup_server, create_server_instance},
};

fn server_with_session_ttl(clock: &MockClock, ttl: Duration) -> Database {
	// Without a mock clock the test would have to sleep out the whole session ttl.
	let _ = rustls::crypto::ring::default_provider().install_default();
	let mut config = RuntimeConfig::default().seeded(0);
	config.clock = Clock::Mock(clock.clone());
	server::memory()
		.with_runtime_config(config)
		.with_auth(move |auth| auth.session_ttl(ttl))
		.with_grpc(|grpc| grpc.admin_bind_addr("[::1]:0"))
		.with_http(|http| http.admin_bind_addr("::1:0"))
		.with_ws(|ws| ws.admin_bind_addr("::1:0"))
		.build()
		.unwrap()
}

fn wallet_keypair(seed: u8) -> (SigningKey, String) {
	// A fixed seed keeps the wallet address stable so a failure stays reproducible.
	let signing_key = SigningKey::from_bytes(&[seed; 32]);
	let public_key = bs58_encode(signing_key.verifying_key().as_bytes()).into_string();
	(signing_key, public_key)
}

async fn begin_wallet_challenge_over_http(port: u16, public_key: &str) -> (String, String) {
	// The typed http client drops the challenge id, so this must speak the wire directly.
	let response: JsonValue = reqwest::Client::new()
		.post(format!("http://[::1]:{}/v1/authenticate", port))
		.json(&json!({
			"method": "solana",
			"credentials": { "identifier": public_key, "public_key": public_key }
		}))
		.send()
		.await
		.unwrap()
		.json()
		.await
		.unwrap();

	assert_eq!(
		response["status"], "challenge",
		"a wallet login over http must answer with a signing challenge, got {response}"
	);

	(
		response["challenge_id"].as_str().expect("a challenge must carry an id").to_string(),
		response["payload"]["message"].as_str().expect("a signing challenge must carry a message").to_string(),
	)
}

#[test]
fn test_configured_session_ttl_governs_tokens_minted_over_http() {
	// A transport minting 24 hour tokens under a 60 second ttl has silently dropped the configuration.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let clock = MockClock::from_millis(1_700_000_000_000);
	let mut server = server_with_session_ttl(&clock, Duration::from_seconds(60).unwrap());
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			HttpClient::connect(&format!("http://[::1]:{}", http_port), WireFormat::Frames).await.unwrap();
		let token = client.login_with_password("alice", "alice-pass").await.unwrap().token;

		clock.advance_secs(59);
		assert!(
			client.login_with_token(&token).await.is_ok(),
			"a session token must still be accepted inside the configured 60 second ttl"
		);

		clock.advance_secs(2);
		assert!(
			client.login_with_token(&token).await.is_err(),
			"a session token must expire 60 seconds after issue, not after the 24 hour default"
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_challenge_issued_over_http_completes_over_websocket() {
	// A per-transport challenge store strands any client that answers on a different connection.
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (ws_port, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let (signing_key, public_key) = wallet_keypair(7);
		let (challenge_id, message) = begin_wallet_challenge_over_http(http_port, &public_key).await;

		let signature = bs58_encode(signing_key.sign(message.as_bytes()).to_bytes()).into_string();
		let mut client =
			WsClient::connect(&format!("ws://[::1]:{}", ws_port), WireFormat::Frames).await.unwrap();

		let login = client
			.login(
				"solana",
				HashMap::from([
					("challenge_id".to_string(), challenge_id),
					("signature".to_string(), signature),
					("signed_message".to_string(), message),
				]),
			)
			.await
			.expect("the websocket transport must resolve a challenge issued over http");

		assert!(!login.token.is_empty(), "completing the challenge must mint a session token");
		assert!(!login.identity.is_empty(), "completing the challenge must name the provisioned identity");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
