// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Shared Runtime Demo
//!
//! This example demonstrates the new shared runtime architecture that uses
//! static dispatch and automatic context transformation. Key features:
//!
//! 1. **Zero-Cost Abstractions**: Uses static dispatch with associated types
//! 2. **Automatic Runtime Activation**: Adding `.with_grpc_server()` or 
//!    `.with_websocket_server()` automatically transforms from SyncContext 
//!    to TokioContext
//! 3. **Shared Runtime**: All network servers share a single tokio runtime
//! 4. **Custom Runtime Support**: Users can inject their own runtime implementations
//! 5. **Type-State Pattern**: Compile-time enforcement of context capabilities
//!
//! Note: This example requires the "server" feature to be enabled.

// #[cfg(all(feature = "server", feature = "websocket", feature = "grpc"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use reifydb::ReifyDB;
    use reifydb::network::grpc::server::GrpcConfig;
    use reifydb::network::ws::server::WsConfig;
    use reifydb_system::SystemContext;
    use std::sync::Arc;
    use std::time::Duration;

    println!("=== ReifyDB Shared Runtime Architecture Demo ===\n");

    // Example 1: Automatic context transformation
    println!("🚀 Example 1: Automatic Context Transformation");
    println!("   Starting with SyncContext, automatically transforms to async when servers are added\n");

    let system_builder = ReifyDB::system()
        .production_config(); // This returns SystemBuilder<VT, UT, SyncContext>

    println!("   • Initial context supports async: {}", system_builder.context().supports_async());

    // Adding network servers automatically transforms to TokioContext
    let async_system = system_builder
        .with_grpc_server(GrpcConfig::default())    // Transforms to TokioContext automatically!
        .with_websocket_server(WsConfig::default()); // Uses shared runtime

    println!("   • After adding servers, context supports async: {}", async_system.context().supports_async());
    println!("   • Both servers now share a single Tokio runtime!\n");

    let mut system = async_system.build();

    println!("   • Created system with {} subsystems", system.subsystem_count());
    println!("   • Subsystems: {:?}\n", system.get_subsystem_names());

    // Start all servers (they share the same runtime)
    system.start()?;
    println!("   ✅ All servers started using shared runtime!");

    // Brief operation
    std::thread::sleep(Duration::from_secs(1));

    system.stop()?;
    println!("   ✅ All servers stopped gracefully!\n");

    // Example 2: Explicit runtime management
    println!("🎛️  Example 2: Explicit Runtime Management");
    println!("   Manually controlling when async context is activated\n");

    let explicit_system = ReifyDB::system()
        .with_async_runtime() // Explicitly activate async context first
        .production_config()
        .with_grpc_server(GrpcConfig::default())
        .with_websocket_server(WsConfig::default())
        .build();

    println!("   • Explicitly activated async context before adding servers");
    println!("   • All servers use the same shared runtime instance\n");

    // Example 3: Custom runtime injection
    println!("⚙️  Example 3: Custom Runtime Injection");
    println!("   Using a user-provided tokio runtime with custom configuration\n");

    // Create a custom runtime with specific configuration
    let custom_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2) // Custom thread count
            .thread_name("custom-reifydb-worker")
            .enable_all()
            .build()?
    );

    let custom_system = ReifyDB::system()
        .with_custom_runtime(custom_runtime.clone()) // Inject our custom runtime
        .production_config()
        .with_grpc_server(GrpcConfig::default())
        .with_websocket_server(WsConfig::default())
        .build();

    println!("   • Injected custom runtime with 2 worker threads");
    println!("   • Both servers use the injected runtime instance\n");

    // Example 4: Runtime sharing benefits
    println!("💡 Runtime Sharing Benefits:");
    println!("   • No duplicate runtime creation (saves memory and threads)");
    println!("   • Better resource utilization across all async subsystems");
    println!("   • Centralized async task management");
    println!("   • Zero-cost abstractions with compile-time optimization");
    println!("   • Type-safe context management prevents runtime errors\n");

    println!("🎯 Key Architecture Features:");
    println!("   • Static dispatch for zero-cost abstractions");
    println!("   • Automatic context transformation when servers are added");
    println!("   • Shared runtime prevents resource duplication");
    println!("   • Custom runtime injection for advanced use cases");
    println!("   • Compile-time enforcement of context capabilities");

    Ok(())
}

#[cfg(not(feature = "server"))]
fn main() {
    println!("=== Shared Runtime Demo ===");
    println!("❌ This demo requires the 'server' feature to be enabled.");
    println!("💡 Run with: cargo run --example shared_runtime_demo --features server");
}