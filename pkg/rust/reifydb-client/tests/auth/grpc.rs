// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_client::{Encoding, GrpcClient};
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
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
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
fn test_password_login_wrong_password() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let result = client.login_with_password("alice", "wrong-password").await;
		assert!(result.is_err(), "Should fail with wrong password");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_password_login_unknown_user() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let result = client.login_with_password("nonexistent", "password").await;
		assert!(result.is_err(), "Should fail with unknown user");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_token_login_success() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let result = client.login_with_token("bob-secret-token").await.unwrap();

		assert!(!result.token.is_empty(), "Token should not be empty");
		assert!(!result.identity.is_empty(), "Identity should not be empty");

		// Verify authenticated queries work
		let query_result = client.query("MAP {v: 42}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_token_login_wrong_token() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let result = client.login_with_token("wrong-token").await;
		assert!(result.is_err(), "Should fail with wrong token");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_sequential_logins() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();

		// Login as alice
		let result_a = client.login_with_password("alice", "alice-pass").await.unwrap();
		assert!(!result_a.token.is_empty());

		// Verify query works as alice
		let query_result = client.query("MAP {v: 1}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);

		// Login as bob (replaces alice session)
		let result_b = client.login_with_token("bob-secret-token").await.unwrap();
		assert!(!result_b.token.is_empty());
		assert_ne!(result_a.token, result_b.token);

		// Verify query works as bob
		let query_result = client.query("MAP {v: 2}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_logout_success() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let result = client.login_with_password("alice", "alice-pass").await.unwrap();
		let old_token = result.token.clone();

		// Verify query works before logout
		let query_result = client.query("MAP {v: 1}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);

		// Logout
		client.logout().await.unwrap();

		// Verify the old token is revoked server-side
		let mut client2 =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		client2.authenticate(&old_token);
		let result = client2.query("MAP {v: 2}", None).await;
		assert!(result.is_err(), "Old token should be revoked after logout");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_logout_twice() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		client.login_with_password("alice", "alice-pass").await.unwrap();

		// First logout
		client.logout().await.unwrap();

		// Second logout should be a no-op (no token set)
		client.logout().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_logout_without_token() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();

		// Logout without ever logging in should be a no-op
		client.logout().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_logout_invalid_token() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();

		// Set an invalid token manually
		client.authenticate("invalid-token-that-does-not-exist");

		// Logout with invalid token should fail
		let result = client.logout().await;
		assert!(result.is_err(), "Logout with invalid token should fail");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_logout_independent_sessions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let (_, grpc_port, _) = start_server_with_auth_users(&mut server).unwrap();

	runtime.block_on(async {
		let mut client_a =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();
		let mut client_b =
			GrpcClient::connect(&format!("http://[::1]:{}", grpc_port), Encoding::Proto).await.unwrap();

		// Both login as alice (separate sessions)
		client_a.login_with_password("alice", "alice-pass").await.unwrap();
		client_b.login_with_password("alice", "alice-pass").await.unwrap();

		// Logout client_a
		client_a.logout().await.unwrap();

		// client_b should still work
		let query_result = client_b.query("MAP {v: 42}", None).await.unwrap();
		assert_eq!(query_result.frames.len(), 1);
	});

	cleanup_server(Some(server));
}
