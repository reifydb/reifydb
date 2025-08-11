// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Server System Demo
//!
//! This example demonstrates how to use the ReifyDB system architecture
//! to manage network servers (gRPC and WebSocket) as subsystems alongside
//! other components like the FlowSubsystem.
//!
//! Note: This example requires the "server" feature to be enabled.

#[cfg(feature = "server")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use reifydb::ReifyDB;
    use reifydb::network::grpc::server::GrpcConfig;
    use reifydb::network::ws::server::WsConfig;
    use std::time::Duration;

    println!("=== ReifyDB Server System Demo ===\n");

    // Create a system with gRPC and WebSocket servers as subsystems
    let mut system = ReifyDB::system()
        .production_config()  // Longer timeouts for production
        // .with_flow_subsystem(Duration::from_secs(1))  // Add FlowSubsystem
        .with_grpc_server(GrpcConfig::default())  // Add gRPC server
        .with_websocket_server(WsConfig::default())  // Add WebSocket server
        .build();

    // println!("Created server system with {} subsystems", system.subsystem_count());
    // println!("Subsystems: {:?}", system.get_subsystem_names());
    // println!("Initial health: {:?}\n", system.health_status());

    // Start all servers and subsystems
    println!("=== Starting All Servers ===");
    system.start()?;
    println!("✅ All servers started!");

    // Show running status
    println!("\n=== Server Status ===");
    // for (name, health) in system.get_all_component_health() {
    //     let status_icon = if health.is_running { "🟢" } else { "🔴" };
    //     println!("  {} {}: {:?}", status_icon, name, health.status);
    // }

    println!("\n🌐 Servers are now running and accepting connections!");
    println!("  • gRPC server: Available for database operations");
    println!("  • WebSocket server: Available for real-time queries");
    println!("  • Flow subsystem: Processing CDC events");
    
    // Let servers run for a short time
    println!("\n⏱️ Letting servers run for 3 seconds...");
    std::thread::sleep(Duration::from_secs(3));

    // Graceful shutdown of all servers
    println!("\n=== Shutting Down All Servers ===");
    system.stop()?;
    println!("✅ All servers stopped gracefully!");

    println!("\n=== Final Status ===");
    // for (name, health) in system.get_all_component_health() {
    //     let status_icon = if health.is_running { "🟢" } else { "⚪" };
    //     println!("  {} {}: {:?}", status_icon, name, health.status);
    // }

    println!("\n🎯 Benefits of the System Architecture:");
    println!("   • All servers start/stop together");
    println!("   • Centralized health monitoring");
    println!("   • Graceful shutdown coordination");
    println!("   • Easy to add new server types");
    
    Ok(())
}

#[cfg(not(feature = "server"))]
fn main() {
    println!("=== Server System Demo ===");
    println!("❌ This demo requires the 'server' feature to be enabled.");
    println!("💡 Run with: cargo run --example server_system_demo --features server");
}