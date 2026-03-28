// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! # Router Example — Query Forwarding
//!
//! A ReifyDB instance that creates a remote namespace pointing to the data server,
//! then demonstrates transparent query forwarding over gRPC.
//!
//! Start the data server first: `cargo run --bin router-data`
//! Then run this: `cargo run --bin router`

use reifydb::{Params, WithSubsystem, server, sub_server_grpc::factory::GrpcConfig};
use tracing::info;

fn main() {
	let mut db = server::memory()
		.with_tracing(|c| c.with_console(|f| f.color(true)))
		.with_grpc(GrpcConfig::default().bind_addr("[::1]:50051"))
		.build()
		.unwrap();

	db.start().unwrap();

	let port = db.sub_server_grpc().unwrap().port().unwrap();
	info!("Router gRPC listening on [::1]:{}", port);

	// Create a remote namespace that points to the data server
	info!("Creating remote namespace store -> [::1]:50052");
	db.admin_as_root(
		"CREATE REMOTE NAMESPACE store WITH { grpc: 'http://[::1]:50052', token: 'service-token' };",
		Params::None,
	)
	.unwrap();

	// Query 1: FROM store::products — transparently forwarded to data server
	info!("--- Query: FROM store::products ---");
	match db.query_as_root("FROM store::products", Params::None) {
		Ok(frames) => {
			for frame in &frames {
				info!("{}", frame);
			}
		}
		Err(e) => info!("Error: {}", e),
	}

	// Query 2: FROM store::products with filter — forwarded filter
	info!("--- Query: FROM store::products FILTER price > 50 ---");
	match db.query_as_root("FROM store::products FILTER price > 50", Params::None) {
		Ok(frames) => {
			for frame in &frames {
				info!("{}", frame);
			}
		}
		Err(e) => info!("Error: {}", e),
	}

	// Query 3: Try INSERT into remote namespace — should get REMOTE_002 error
	info!("--- Command: INSERT into remote namespace (expect REMOTE_002 error) ---");
	match db.command_as_root(r#"INSERT store::products [{ id: 6, name: "Cable", price: 9.99 }];"#, Params::None) {
		Ok(frames) => {
			for frame in &frames {
				info!("Unexpected success: {}", frame);
			}
		}
		Err(e) => info!("Expected error: {}", e),
	}

	info!("Router demo complete.");
}
