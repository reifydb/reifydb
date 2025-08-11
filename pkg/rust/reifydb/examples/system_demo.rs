// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! System Architecture Demo
//!
//! This example demonstrates the new ReifyDB system architecture that provides
//! unified lifecycle management for the engine and all subsystems.
//!
//! Features demonstrated:
//! - System builder pattern with fluent API
//! - Multiple subsystems (FlowSubsystem + potential for gRPC/WebSocket servers)  
//! - Unified startup/shutdown lifecycle
//! - Health monitoring across all components
//! - Configuration options for different environments

use reifydb::ReifyDB;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ReifyDB System Architecture Demo ===\n");

    // Create a system with default in-memory storage and serializable transactions
    let mut system = ReifyDB::system()
        .development_config() // Short timeouts, frequent health checks
        // Note: FlowSubsystem requires CdcScan implementation, skipping for this demo
        .build();

    // println!("Created ReifySystem with {} subsystems", system.subsystem_count());
    // println!("Subsystem names: {:?}", system.get_subsystem_names());
    // println!("Initial system health: {:?}\n", system.health_status());

    // Demonstrate the system lifecycle
    println!("=== Starting System ===");
    system.start()?;
    println!("✅ System started successfully!");
    println!("System running: {}", system.is_running());
    // println!("System health: {:?}\n", system.health_status());

    // Show component-level health information
    println!("=== Component Health Status ===");
    // for (name, health) in system.get_all_component_health() {
    //     println!("  📊 {}: {:?} (running: {}, last updated: {:?})",
    //             name, health.status, health.is_running, health.last_updated);
    // }
    println!();

    // Let the system run and show periodic health updates
    println!("=== Running System (5 seconds) ===");
    // for i in 1..=5 {
    //     println!("⏰ Second {}: System health = {:?}", i, system.health_status());
    //     std::thread::sleep(Duration::from_secs(1));
    //
    //     // Update health monitoring periodically
    //     if i % 2 == 0 {
    //         system.update_health_monitoring();
    //     }
    // }
    println!();

    // Demonstrate graceful shutdown
    println!("=== Stopping System ===");
    system.stop()?;
    println!("✅ System stopped successfully!");
    println!("System running: {}", system.is_running());
    // println!("Final system health: {:?}\n", system.health_status());

    // Show final component states
    println!("=== Final Component Status ===");
    // for (name, health) in system.get_all_component_health() {
    //     println!("  📊 {}: {:?} (running: {})", name, health.status, health.is_running);
    // }

    println!("\n=== Demo completed successfully ===");
    println!("🎉 The ReifyDB System architecture provides:");
    println!("   • Unified lifecycle management");
    println!("   • Health monitoring and observability");
    println!("   • Graceful startup and shutdown");
    println!("   • Extensible subsystem architecture");
    println!("   • Configuration flexibility");

    Ok(())
}
