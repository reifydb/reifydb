// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! # Router Example — Data Server
//!
//! A ReifyDB instance that serves real data over gRPC.
//! Creates a `store` namespace with a `store::products` table and listens on port 50052.
//!
//! Run with: `cargo run --bin router-data`
//! Then in another terminal: `cargo run --bin router`

use reifydb::{Params, WithSubsystem, server};
use tracing::info;

fn main() {
	let mut db = server::memory()
		.with_tracing(|c| c.with_console(|f| f.color(true)))
		.with_grpc(|c| c.bind_addr("[::1]:50052"))
		.build()
		.unwrap();

	db.start().unwrap();

	let port = db.sub_server_grpc().unwrap().port().unwrap();
	info!("Data server gRPC listening on [::1]:{}", port);

	// Create a service token so the router can authenticate
	db.admin_as_root("CREATE AUTHENTICATION FOR root { method: token; token: 'service-token' }", Params::None)
		.unwrap();

	// Create namespace and table
	db.admin_as_root("CREATE NAMESPACE store;", Params::None).unwrap();
	db.admin_as_root(
		r#"
		CREATE TABLE store::products {
			id: int4,
			name: utf8,
			price: float8
		};
		"#,
		Params::None,
	)
	.unwrap();

	// Insert sample data
	db.command_as_root(
		r#"
		INSERT store::products [
			{ id: 1, name: "Keyboard", price: 49.99 },
			{ id: 2, name: "Mouse", price: 29.99 },
			{ id: 3, name: "Monitor", price: 299.99 },
			{ id: 4, name: "Headphones", price: 19.99 },
			{ id: 5, name: "Webcam", price: 59.99 }
		];
		"#,
		Params::None,
	)
	.unwrap();

	info!("Data server ready with store::products (5 rows)");
	info!("Waiting for connections... Press Ctrl+C to stop.");

	db.await_signal().unwrap();
}
