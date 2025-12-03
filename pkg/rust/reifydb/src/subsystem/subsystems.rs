// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::TypeId,
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::Result;
use reifydb_sub_api::{HealthStatus, Subsystem};
use tracing::{debug, error, warn};

use crate::health::HealthMonitor;

pub struct Subsystems {
	subsystems: Vec<Box<dyn Subsystem>>,
	index: HashMap<TypeId, usize>,
	running: Arc<AtomicBool>,
	health_monitor: Arc<HealthMonitor>,
}

impl Subsystems {
	pub fn new(health_monitor: Arc<HealthMonitor>) -> Self {
		Self {
			subsystems: Vec::new(),
			index: HashMap::new(),
			running: Arc::new(AtomicBool::new(false)),
			health_monitor,
		}
	}

	/// Add a subsystem to be managed
	pub fn add_subsystem(&mut self, subsystem: Box<dyn Subsystem>) {
		self.health_monitor.update_component_health(
			subsystem.name().to_string(),
			subsystem.health_status(),
			subsystem.is_running(),
		);

		let type_id = (*subsystem).as_any().type_id();

		let index = self.subsystems.len();
		self.index.insert(type_id, index);

		self.subsystems.push(subsystem);
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.len()
	}

	pub fn start_all(&mut self, startup_timeout: Duration) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		debug!("Starting {} subsystems...", self.subsystems.len());

		let start_time = std::time::Instant::now();
		let mut started_subsystems = Vec::new();

		for subsystem in &mut self.subsystems {
			if start_time.elapsed() > startup_timeout {
				error!("Startup timeout exceeded");
				self.stop_started_subsystems(&started_subsystems)?;
				panic!("Startup timeout exceeded");
			}

			let name = subsystem.name().to_string();
			debug!("Starting subsystem: {}", name);

			match subsystem.start() {
				Ok(()) => {
					self.health_monitor.update_component_health(
						name.clone(),
						subsystem.health_status(),
						subsystem.is_running(),
					);
					started_subsystems.push(name.clone());
					debug!("Successfully started: {}", name);
				}
				Err(e) => {
					error!("Failed to start subsystem '{}': {}", name, e);
					self.health_monitor.update_component_health(
						name.clone(),
						HealthStatus::Failed {
							description: format!("Startup failed: {}", e),
						},
						false,
					);
					self.stop_started_subsystems(&started_subsystems)?;
					return Err(e);
				}
			}
		}

		self.running.store(true, Ordering::Relaxed);
		debug!("All {} subsystems started successfully", started_subsystems.len());
		Ok(())
	}

	pub fn stop_all(&mut self, shutdown_timeout: Duration) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		debug!("Stopping {} subsystems...", self.subsystems.len());

		for subsystem in self.subsystems.iter().rev() {
			debug!("Stopping subsystem: {}", subsystem.name());
		}

		let start_time = std::time::Instant::now();
		let mut errors = Vec::new();

		for subsystem in self.subsystems.iter_mut().rev() {
			if start_time.elapsed() > shutdown_timeout {
				warn!("Shutdown timeout exceeded");
				break;
			}

			let name = subsystem.name().to_string();

			match subsystem.shutdown() {
				Ok(()) => {
					// Update health monitoring
					self.health_monitor.update_component_health(
						name.clone(),
						subsystem.health_status(),
						subsystem.is_running(),
					);
					debug!("Successfully stopped: {}", name);
				}
				Err(e) => {
					error!("Error stopping subsystem '{}': {}", name, e);
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
			debug!("All subsystems stopped successfully");
			Ok(())
		} else {
			let error_msg =
				format!("Errors occurred while stopping {} subsystems: {:?}", errors.len(), errors);
			error!("{}", error_msg);
			panic!("Errors occurred during shutdown: {:?}", errors)
		}
	}

	pub fn update_health_monitoring(&mut self) {
		for subsystem in &self.subsystems {
			self.health_monitor.update_component_health(
				subsystem.name().to_string(),
				subsystem.health_status(),
				subsystem.is_running(),
			);
		}
	}

	pub fn get_subsystem_names(&self) -> Vec<String> {
		self.subsystems.iter().map(|subsystem| subsystem.name().to_string()).collect()
	}

	pub fn get<T: 'static>(&self) -> Option<&T> {
		let type_id = TypeId::of::<T>();
		let index = *self.index.get(&type_id)?;
		self.subsystems.get(index)?.as_any().downcast_ref::<T>()
	}

	#[allow(dead_code)]
	pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
		let type_id = TypeId::of::<T>();
		let index = *self.index.get(&type_id)?;
		self.subsystems.get_mut(index)?.as_any_mut().downcast_mut::<T>()
	}

	fn stop_started_subsystems(&mut self, started_names: &[String]) -> Result<()> {
		let mut errors = Vec::new();

		// Stop the started subsystems in reverse order
		for name in started_names.iter().rev() {
			// Find and stop the subsystem by name
			for subsystem in &mut self.subsystems {
				if subsystem.name() == name {
					if let Err(e) = subsystem.shutdown() {
						error!("Error stopping '{}' during rollback: {}", name, e);
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

		if errors.is_empty() {
			Ok(())
		} else {
			panic!("Rollback errors: {:?}", errors)
		}
	}
}
