// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::health::{HealthMonitor, HealthStatus};
use crate::Subsystem;
use reifydb_core::Result;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

/// Manages the lifecycle of multiple subsystems
///
/// The SubsystemManager coordinates the starting and stopping of subsystems
/// in a controlled manner, handles errors, and provides health monitoring.
pub struct SubsystemManager {
    /// Collection of managed subsystems, keyed by their TypeId for efficient lookup
    subsystems: HashMap<TypeId, Box<dyn Subsystem>>,
    /// Whether the manager is currently running
    running: Arc<AtomicBool>,
    /// Health monitor for tracking subsystem status
    health_monitor: Arc<HealthMonitor>,
}

impl SubsystemManager {
    /// Create a new subsystem manager
    pub fn new(health_monitor: Arc<HealthMonitor>) -> Self {
        Self { 
            subsystems: HashMap::new(), 
            running: Arc::new(AtomicBool::new(false)), 
            health_monitor 
        }
    }

    /// Add a subsystem to be managed
    pub fn add_subsystem(&mut self, subsystem: Box<dyn Subsystem>) {
        // Initialize health monitoring for the subsystem
        self.health_monitor.update_component_health(
            subsystem.name().to_string(),
            subsystem.health_status(),
            subsystem.is_running(),
        );

        // Get the TypeId of the concrete type
        let type_id = (*subsystem).as_any().type_id();
        self.subsystems.insert(type_id, subsystem);
    }

    /// Get the number of managed subsystems
    pub fn subsystem_count(&self) -> usize {
        self.subsystems.len()
    }

    /// Check if the manager is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Start all subsystems sequentially
    ///
    /// Subsystems are started in insertion order. If any subsystem
    /// fails to start, the startup process is aborted and all previously
    /// started subsystems are stopped.
    pub fn start_all(&mut self, startup_timeout: Duration) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already running
        }

        println!("[SubsystemManager] Starting {} subsystems...", self.subsystems.len());

        let start_time = std::time::Instant::now();
        let mut started_subsystems = Vec::new();

        for (_type_id, subsystem) in &mut self.subsystems {
            // Check timeout
            if start_time.elapsed() > startup_timeout {
                eprintln!("[SubsystemManager] Startup timeout exceeded");
                // Rollback: stop all previously started subsystems
                self.stop_started_subsystems(&started_subsystems)?;
                panic!("Startup timeout exceeded");
            }

            let name = subsystem.name().to_string();
            println!("[SubsystemManager] Starting subsystem: {}", name);

            match subsystem.start() {
                Ok(()) => {
                    // Update health monitoring
                    self.health_monitor.update_component_health(
                        name.clone(),
                        subsystem.health_status(),
                        subsystem.is_running(),
                    );
                    started_subsystems.push(name.clone());
                    println!("[SubsystemManager] Successfully started: {}", name);
                }
                Err(e) => {
                    eprintln!("[SubsystemManager] Failed to start subsystem '{}': {}", name, e);
                    // Update health monitoring with failure
                    self.health_monitor.update_component_health(
                        name.clone(),
                        HealthStatus::Failed {
                            description: format!("Startup failed: {}", e),
                        },
                        false,
                    );
                    // Rollback: stop all previously started subsystems
                    self.stop_started_subsystems(&started_subsystems)?;
                    return Err(e);
                }
            }
        }

        self.running.store(true, Ordering::Relaxed);
        println!("[SubsystemManager] All {} subsystems started successfully", started_subsystems.len());
        Ok(())
    }

    /// Stop all subsystems
    ///
    /// Subsystems are stopped in arbitrary order (HashMap iteration order).
    /// Errors during shutdown are logged but don't prevent other subsystems
    /// from being stopped.
    pub fn stop_all(&mut self, shutdown_timeout: Duration) -> Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already stopped
        }

        println!("[SubsystemManager] Stopping {} subsystems...", self.subsystems.len());

        let start_time = std::time::Instant::now();
        let mut errors = Vec::new();

        // Stop all subsystems (HashMap doesn't guarantee order)
        for (_type_id, subsystem) in &mut self.subsystems {
            // Check timeout
            if start_time.elapsed() > shutdown_timeout {
                eprintln!("[SubsystemManager] Shutdown timeout exceeded");
                break;
            }

            let name = subsystem.name().to_string();
            println!("[SubsystemManager] Stopping subsystem: {}", name);

            match subsystem.stop() {
                Ok(()) => {
                    // Update health monitoring
                    self.health_monitor.update_component_health(
                        name.clone(),
                        subsystem.health_status(),
                        subsystem.is_running(),
                    );
                    println!("[SubsystemManager] Successfully stopped: {}", name);
                }
                Err(e) => {
                    eprintln!("[SubsystemManager] Error stopping subsystem '{}': {}", name, e);
                    // Update health monitoring with failure
                    self.health_monitor.update_component_health(
                        name.clone(),
                        HealthStatus::Failed {
                            description: format!("Shutdown failed: {}", e),
                        },
                        subsystem.is_running(),
                    );
                    errors.push((name.clone(), e));
                }
            }
        }

        self.running.store(false, Ordering::Relaxed);

        if errors.is_empty() {
            println!("[SubsystemManager] All subsystems stopped successfully");
            Ok(())
        } else {
            let error_msg =
                format!("Errors occurred while stopping {} subsystems: {:?}", errors.len(), errors);
            eprintln!("[SubsystemManager] {}", error_msg);
            panic!("Errors occurred during shutdown: {:?}", errors)
        }
    }

    /// Update health monitoring for all subsystems
    pub fn update_health_monitoring(&mut self) {
        for (_type_id, subsystem) in &self.subsystems {
            self.health_monitor.update_component_health(
                subsystem.name().to_string(),
                subsystem.health_status(),
                subsystem.is_running(),
            );
        }
    }

    /// Get the names of all managed subsystems
    pub fn get_subsystem_names(&self) -> Vec<String> {
        self.subsystems.iter().map(|(_type_id, subsystem)| subsystem.name().to_string()).collect()
    }

    /// Get a reference to a subsystem of a specific type
    ///
    /// This method uses the TypeId for O(1) lookup efficiency.
    /// Returns the subsystem if found and successfully downcasted, or None otherwise.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        let type_id = TypeId::of::<T>();
        self.subsystems.get(&type_id)?.as_any().downcast_ref::<T>()
    }

    /// Stop subsystems by name (used for rollback during failed startup)
    fn stop_started_subsystems(&mut self, started_names: &[String]) -> Result<()> {
        let mut errors = Vec::new();

        // Stop the started subsystems in reverse order
        for name in started_names.iter().rev() {
            // Find and stop the subsystem by name
            for (_type_id, subsystem) in &mut self.subsystems {
                if subsystem.name() == name {
                    if let Err(e) = subsystem.stop() {
                        eprintln!(
                            "[SubsystemManager] Error stopping '{}' during rollback: {}",
                            name, e
                        );
                        errors.push((name.clone(), e));
                    }
                    // Update health monitoring
                    self.health_monitor.update_component_health(
                        name.clone(),
                        subsystem.health_status(),
                        subsystem.is_running(),
                    );
                    break;
                }
            }
        }

        if errors.is_empty() { Ok(()) } else { panic!("Rollback errors: {:?}", errors) }
    }
}