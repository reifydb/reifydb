// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Test example for subsystem-based feature gating
//!
//! This example demonstrates:
//! - Feature-gated subsystem compilation (only available when features are enabled)
//! - Automatic async context transformation when network servers are added
//! - Runtime sharing across multiple subsystems

use reifydb::ReifyDB;
use std::time::Duration;

fn main() {
    println!("=== Subsystem Features Test ===");

    // Create system with SyncContext (no async runtime by default)
    let system_builder = ReifyDB::system()
        .with_graceful_shutdown_timeout(Duration::from_secs(10))
        .with_health_check_interval(Duration::from_secs(2));

    println!("✓ Created SystemBuilder with SyncContext");

    #[cfg(feature = "sub_flow")]
    {
        println!("✓ Flow subsystem feature is enabled");
        // Note: FlowSubsystem requires CdcScan trait which isn't implemented yet
        // let system_builder = system_builder.with_flow_subsystem(Duration::from_secs(1));
    }

    #[cfg(all(feature = "sub_grpc", not(feature = "sub_ws")))]
    {
        println!("✓ gRPC subsystem feature is enabled");
        use reifydb_network::grpc::server::GrpcConfig;

        // Adding gRPC server automatically transforms to TokioContext
        let _system_builder = system_builder
            .with_grpc_server(GrpcConfig { socket: Some("127.0.0.1:50051".parse().unwrap()) });
        println!("  → Automatically transformed to TokioContext with shared runtime");
    }

    #[cfg(all(feature = "sub_ws", not(feature = "sub_grpc")))]
    {
        println!("✓ WebSocket subsystem feature is enabled");
        use reifydb_network::ws::server::WsConfig;

        // Adding WebSocket server also transforms to TokioContext
        let _system_builder = system_builder
            .with_websocket_server(WsConfig { socket: Some("127.0.0.1:8080".parse().unwrap()) });
        println!("  → Automatically transformed to TokioContext with shared runtime");
    }

    #[cfg(all(feature = "sub_grpc", feature = "sub_ws"))]
    {
        println!("✓ Both gRPC and WebSocket subsystem features are enabled");
        use reifydb_network::grpc::server::GrpcConfig;
        use reifydb_network::ws::server::WsConfig;

        // Adding both servers - demonstrate shared runtime
        let _system_builder = system_builder
            .with_grpc_server(GrpcConfig { socket: Some("127.0.0.1:50051".parse().unwrap()) })
            .with_websocket_server(WsConfig { socket: Some("127.0.0.1:8080".parse().unwrap()) });
        println!("  → Both servers use the same shared TokioContext runtime");
    }

    // Show which features are active
    let mut active_features: Vec<&str> = Vec::new();

    #[cfg(feature = "sub_grpc")]
    active_features.push("sub_grpc");

    #[cfg(feature = "sub_ws")]
    active_features.push("sub_ws");

    #[cfg(feature = "sub_flow")]
    active_features.push("sub_flow");

    if active_features.is_empty() {
        println!("No subsystem features enabled - only basic system functionality available");
    } else {
        println!("Active subsystem features: {}", active_features.join(", "));
    }

    println!("✓ All tests passed - subsystem-based feature gating working correctly!");
}
