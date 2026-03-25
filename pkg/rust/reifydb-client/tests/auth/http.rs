// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::HttpClient;
use tokio::runtime::Runtime;

use crate::{
	auth::start_server_with_auth_users,
	common::{cleanup_server, create_server_instance},
};

#[test]
fn test_password_login_success() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let result = client.login_with_password("alice", "alice-pass").await.unwrap();

		assert!(!result.token.is_empty(), "Token should not be empty");
		assert!(!result.identity.is_empty(), "Identity should not be empty");

		// Verify authenticated queries work
		let query_result = client.query("MAP {v: 42}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}

#[test]
#[should_panic]
fn test_password_login_wrong_password() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let _ = client.login_with_password("alice", "wrong-password").await;
	});

	cleanup_server(Some(server));
}

#[test]
#[should_panic]
fn test_password_login_unknown_user() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let _ = client.login_with_password("nonexistent", "password").await;
	});

	cleanup_server(Some(server));
}

#[test]
fn test_token_login_success() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let result = client.login_with_token("bob", "bob-secret-token").await.unwrap();

		assert!(!result.token.is_empty(), "Token should not be empty");
		assert!(!result.identity.is_empty(), "Identity should not be empty");

		// Verify authenticated queries work
		let query_result = client.query("MAP {v: 42}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}

#[test]
#[should_panic]
fn test_token_login_wrong_token() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let _ = client.login_with_token("bob", "wrong-token").await;
	});

	cleanup_server(Some(server));
}

#[test]
#[should_panic]
fn test_token_login_unknown_user() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();
		let _ = client.login_with_token("nonexistent", "some-token").await;
	});

	cleanup_server(Some(server));
}

#[test]
fn test_sequential_logins() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, _, http_port) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client = HttpClient::connect(&format!("http://[::1]:{}", http_port)).await.unwrap();

		// Login as alice
		let result_a = client.login_with_password("alice", "alice-pass").await.unwrap();
		assert!(!result_a.token.is_empty());

		// Verify query works as alice
		let query_result = client.query("MAP {v: 1}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);

		// Login as bob (replaces alice session)
		let result_b = client.login_with_token("bob", "bob-secret-token").await.unwrap();
		assert!(!result_b.token.is_empty());
		assert_ne!(result_a.token, result_b.token);

		// Verify query works as bob
		let query_result = client.query("MAP {v: 2}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}
