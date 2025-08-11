// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Basic example demonstrating the ReifySystem architecture
//!
//! This example shows how to:
//! - Create a ReifySystem with Engine + FlowSubsystem
//! - Use the builder pattern for configuration
//! - Start and stop the system
//! - Monitor health status

use reifydb_core::hook::Hooks;
use reifydb_engine::Engine;
use reifydb_system::{ReifySystemBuilder, FlowSubsystemAdapter};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb_transaction::svl::SingleVersionLock;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ReifySystem Basic Example ===\n");

    // Create storage and transaction layers
    let storage = Memory::new();
    let hooks = Hooks::new();
    let unversioned = SingleVersionLock::new(storage.clone(), hooks.clone());
    let versioned = Optimistic::new(storage, unversioned.clone(), hooks.clone());

    // Create the engine
    let engine = Engine::new(versioned, unversioned, hooks)?;
    
    // Create a FlowSubsystem
    let flow_subsystem = engine.create_flow_subsystem(Duration::from_millis(500));
    let flow_adapter = FlowSubsystemAdapter::new(flow_subsystem);

    // Build the system using the builder pattern
    let mut system = ReifySystemBuilder::new(engine)
        .development_config()  // Use development configuration
        .add_subsystem(Box::new(flow_adapter))
        .build();

    println!("Created ReifySystem with {} subsystems", system.subsystem_count());
    println!("Initial system health: {:?}\n", system.health_status());

    // Start the system
    println!("Starting system...");
    system.start()?;
    println!("System started successfully!");
    println!("System running: {}", system.is_running());
    println!("System health: {:?}\n", system.health_status());

    // Let the system run for a bit
    println!("Letting system run for 2 seconds...");
    std::thread::sleep(Duration::from_secs(2));

    // Check health status
    println!("System health after running: {:?}", system.health_status());
    println!("All component health:");
    for (name, health) in system.get_all_component_health() {
        println!("  {}: {:?} (running: {})", name, health.status, health.is_running);
    }
    println!();

    // Stop the system
    println!("Stopping system...");
    system.stop()?;
    println!("System stopped successfully!");
    println!("System running: {}", system.is_running());
    println!("Final system health: {:?}", system.health_status());

    println!("\n=== Example completed successfully ===");
    Ok(())
}