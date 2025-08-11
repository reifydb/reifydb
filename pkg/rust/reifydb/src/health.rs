// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Health status of a component
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Component is healthy and operating normally
    Healthy,
    /// Component is running but experiencing non-critical issues
    Warning { description: String },
    /// Component is experiencing critical issues but is still running
    Degraded { description: String },
    /// Component has failed and is not operational
    Failed { description: String },
    /// Component status is unknown (e.g., during startup)
    Unknown,
}

impl HealthStatus {
    /// Check if the status represents a healthy state
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if the status represents a failure state
    pub fn is_failed(&self) -> bool {
        matches!(self, HealthStatus::Failed { .. })
    }

    /// Get a human-readable description of the status
    pub fn description(&self) -> &str {
        match self {
            HealthStatus::Healthy => "Healthy",
            HealthStatus::Warning { description: message } => message,
            HealthStatus::Degraded { description: message } => message,
            HealthStatus::Failed { description: message } => message,
            HealthStatus::Unknown => "Unknown",
        }
    }
}

/// System health information for a specific component
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    /// Name of the component
    pub name: String,
    /// Current health status
    pub status: HealthStatus,
    /// Last time the health was updated
    pub last_updated: Instant,
    /// Whether the component is currently running
    pub is_running: bool,
}

/// Monitors and aggregates health status across all system components
#[derive(Debug)]
pub struct HealthMonitor {
    /// Health status of all components
    components: Arc<Mutex<HashMap<String, ComponentHealth>>>,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new() -> Self {
        Self { components: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Update the health status of a component
    pub fn update_component_health(&self, name: String, status: HealthStatus, is_running: bool) {
        let mut components = self.components.lock().unwrap();
        components.insert(
            name.clone(),
            ComponentHealth { name, status, last_updated: Instant::now(), is_running },
        );
    }

    /// Get the health status of a specific component
    pub fn get_component_health(&self, name: &str) -> Option<ComponentHealth> {
        let components = self.components.lock().unwrap();
        components.get(name).cloned()
    }

    /// Get the health status of all components
    pub fn get_all_health(&self) -> HashMap<String, ComponentHealth> {
        let components = self.components.lock().unwrap();
        components.clone()
    }

    /// Get the overall system health status
    ///
    /// The system is considered:
    /// - Healthy: if all components are healthy
    /// - Warning: if any component has warnings but none are degraded/failed
    /// - Degraded: if any component is degraded but none are failed
    /// - Failed: if any component has failed
    /// - Unknown: if any component status is unknown
    pub fn get_system_health(&self) -> HealthStatus {
        let components = self.components.lock().unwrap();

        if components.is_empty() {
            return HealthStatus::Unknown;
        }

        let mut has_warning = false;
        let mut has_degraded = false;
        let mut has_unknown = false;

        for health in components.values() {
            match &health.status {
                HealthStatus::Healthy => continue,
                HealthStatus::Warning { .. } => has_warning = true,
                HealthStatus::Degraded { .. } => has_degraded = true,
                HealthStatus::Failed { description: message } => {
                    return HealthStatus::Failed {
                        description: format!("Component '{}' failed: {}", health.name, message),
                    };
                }
                HealthStatus::Unknown => has_unknown = true,
            }
        }

        if has_unknown {
            HealthStatus::Unknown
        } else if has_degraded {
            HealthStatus::Degraded {
                description: "One or more components are degraded".to_string(),
            }
        } else if has_warning {
            HealthStatus::Warning {
                description: "One or more components have warnings".to_string(),
            }
        } else {
            HealthStatus::Healthy
        }
    }

    /// Remove a component from health monitoring
    pub fn remove_component(&self, name: &str) {
        let mut components = self.components.lock().unwrap();
        components.remove(name);
    }

    /// Check if any components have stale health information
    /// (not updated within the specified duration)
    pub fn get_stale_components(&self, max_age: Duration) -> Vec<String> {
        let components = self.components.lock().unwrap();
        let now = Instant::now();

        components
            .values()
            .filter_map(|health| {
                if now.duration_since(health.last_updated) > max_age {
                    Some(health.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}
