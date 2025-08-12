// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Warning { description: String },
    Degraded { description: String },
    Failed { description: String },
    Unknown,
}

impl HealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, HealthStatus::Failed { .. })
    }

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

#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_updated: Instant,
    pub is_running: bool,
}

#[derive(Debug)]
pub struct HealthMonitor {
    components: Arc<Mutex<HashMap<String, ComponentHealth>>>,
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self { components: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn update_component_health(&self, name: String, status: HealthStatus, is_running: bool) {
        let mut components = self.components.lock().unwrap();
        components.insert(
            name.clone(),
            ComponentHealth { name, status, last_updated: Instant::now(), is_running },
        );
    }

    pub fn get_component_health(&self, name: &str) -> Option<ComponentHealth> {
        let components = self.components.lock().unwrap();
        components.get(name).cloned()
    }

    pub fn get_all_health(&self) -> HashMap<String, ComponentHealth> {
        let components = self.components.lock().unwrap();
        components.clone()
    }

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

    pub fn remove_component(&self, name: &str) {
        let mut components = self.components.lock().unwrap();
        components.remove(name);
    }

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
