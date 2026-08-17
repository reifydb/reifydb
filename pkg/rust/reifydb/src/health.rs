// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, fmt, sync::Arc};

use reifydb_runtime::{
	context::clock::{Clock, Instant},
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::value::duration::Duration;

#[derive(Debug, Clone)]
pub struct ComponentHealth {
	pub name: String,
	pub status: HealthStatus,
	pub last_updated: Instant,
	pub is_running: bool,
}

pub struct HealthMonitor {
	components: Arc<Mutex<HashMap<String, ComponentHealth>>>,
	subsystems: Arc<Mutex<HashMap<String, Arc<dyn Subsystem>>>>,
	clock: Clock,
}

impl fmt::Debug for HealthMonitor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("HealthMonitor").field("components", &self.components).finish_non_exhaustive()
	}
}

impl HealthMonitor {
	pub fn new(clock: Clock) -> Self {
		Self {
			components: Arc::new(Mutex::new(HashMap::new())),
			subsystems: Arc::new(Mutex::new(HashMap::new())),
			clock,
		}
	}

	pub fn register_subsystem(&self, name: String, subsystem: Arc<dyn Subsystem>) {
		self.subsystems.lock().insert(name, subsystem);
	}

	pub fn update_component_health(&self, name: String, status: HealthStatus, is_running: bool) {
		let mut components = self.components.lock();
		components.insert(
			name.clone(),
			ComponentHealth {
				name,
				status,
				last_updated: self.clock.instant(),
				is_running,
			},
		);
	}

	fn live_subsystem_health(&self, name: &str, subsystem: &Arc<dyn Subsystem>) -> ComponentHealth {
		ComponentHealth {
			name: name.to_string(),
			status: subsystem.health_status(),
			last_updated: self.clock.instant(),
			is_running: subsystem.is_running(),
		}
	}

	pub fn get_component_health(&self, name: &str) -> Option<ComponentHealth> {
		if let Some(subsystem) = self.subsystems.lock().get(name) {
			return Some(self.live_subsystem_health(name, subsystem));
		}
		self.components.lock().get(name).cloned()
	}

	pub fn get_all_health(&self) -> HashMap<String, ComponentHealth> {
		let mut result: HashMap<String, ComponentHealth> = self
			.subsystems
			.lock()
			.iter()
			.map(|(name, subsystem)| (name.clone(), self.live_subsystem_health(name, subsystem)))
			.collect();

		for (name, health) in self.components.lock().iter() {
			result.entry(name.clone()).or_insert_with(|| health.clone());
		}

		result
	}

	pub fn get_system_health(&self) -> HealthStatus {
		let components = self.get_all_health();

		if components.is_empty() {
			return HealthStatus::Unknown;
		}

		let mut has_warning = false;
		let mut has_degraded = false;
		let mut has_unknown = false;

		for health in components.values() {
			match &health.status {
				HealthStatus::Healthy => continue,
				HealthStatus::Warning {
					..
				} => has_warning = true,
				HealthStatus::Degraded {
					..
				} => has_degraded = true,
				HealthStatus::Failed {
					description: message,
				} => {
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
		self.subsystems.lock().remove(name);
		self.components.lock().remove(name);
	}

	pub fn get_stale_components(&self, max_age: Duration) -> Vec<String> {
		let components = self.components.lock();
		let now = self.clock.instant();

		components
			.values()
			.filter_map(|health| {
				if now.duration_since(&health.last_updated) > max_age.to_std() {
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
		Self::new(Clock::Real)
	}
}
