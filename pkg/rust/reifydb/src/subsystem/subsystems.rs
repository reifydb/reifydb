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

use crate::{
	health::{HealthMonitor, HealthStatus},
	subsystem::Subsystem,
};

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
		// Initialize health monitoring for the subsystem
		self.health_monitor.update_component_health(
			subsystem.name().to_string(),
			subsystem.health_status(),
			subsystem.is_running(),
		);

		// Get the TypeId of the concrete type
		let type_id = (*subsystem).as_any().type_id();
		
		// Store the index for fast lookup
		let index = self.subsystems.len();
		self.index.insert(type_id, index);
		
		// Add to the ordered list
		self.subsystems.push(subsystem);
	}

	pub fn subsystem_count(&self) -> usize {
		self.subsystems.len()
	}

	pub fn start_all(&mut self, startup_timeout: Duration) -> Result<()> {
		if self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already running
		}

		println!(
			"[Subsystem] Starting {} subsystems...",
			self.subsystems.len()
		);

		let start_time = std::time::Instant::now();
		let mut started_subsystems = Vec::new();

		for subsystem in &mut self.subsystems {
			// Check timeout
			if start_time.elapsed() > startup_timeout {
				println!(
					"[Subsystem] Startup timeout exceeded"
				);
				// Rollback: stop all previously started
				// subsystems
				self.stop_started_subsystems(
					&started_subsystems,
				)?;
				panic!("Startup timeout exceeded");
			}

			let name = subsystem.name().to_string();
			println!(
				"[Subsystem] Starting subsystem: {}",
				name
			);

			match subsystem.start() {
				Ok(()) => {
					// Update health monitoring
					self.health_monitor
						.update_component_health(
							name.clone(),
							subsystem
								.health_status(
								),
							subsystem.is_running(),
						);
					started_subsystems.push(name.clone());
					println!(
						"[Subsystem] Successfully started: {}",
						name
					);
				}
				Err(e) => {
					println!(
						"[Subsystem] Failed to start subsystem '{}': {}",
						name, e
					);
					// Update health monitoring with failure
					self.health_monitor
						.update_component_health(
							name.clone(),
							HealthStatus::Failed {
								description: format!(
									"Startup failed: {}",
									e
								),
							},
							false,
						);
					// Rollback: stop all previously started
					// subsystems
					self.stop_started_subsystems(
						&started_subsystems,
					)?;
					return Err(e);
				}
			}
		}

		self.running.store(true, Ordering::Relaxed);
		println!(
			"[Subsystem] All {} subsystems started successfully",
			started_subsystems.len()
		);
		Ok(())
	}

	pub fn stop_all(&mut self, shutdown_timeout: Duration) -> Result<()> {
		if !self.running.load(Ordering::Relaxed) {
			return Ok(()); // Already stopped
		}

		println!(
			"[Subsystem] Stopping {} subsystems...",
			self.subsystems.len()
		);

		let start_time = std::time::Instant::now();
		let mut errors = Vec::new();

		// Stop all subsystems in reverse order
		for subsystem in self.subsystems.iter_mut().rev() {
			// Check timeout
			if start_time.elapsed() > shutdown_timeout {
				println!(
					"[Subsystem] Shutdown timeout exceeded"
				);
				break;
			}

			let name = subsystem.name().to_string();
			println!(
				"[Subsystem] Stopping subsystem: {}",
				name
			);

			match subsystem.stop() {
				Ok(()) => {
					// Update health monitoring
					self.health_monitor
						.update_component_health(
							name.clone(),
							subsystem
								.health_status(
								),
							subsystem.is_running(),
						);
					println!(
						"[Subsystem] Successfully stopped: {}",
						name
					);
				}
				Err(e) => {
					println!(
						"[Subsystem] Error stopping subsystem '{}': {}",
						name, e
					);
					// Update health monitoring with failure
					self.health_monitor
						.update_component_health(
							name.clone(),
							HealthStatus::Failed {
								description: format!(
									"Shutdown failed: {}",
									e
								),
							},
							subsystem.is_running(),
						);
					errors.push((name.clone(), e));
				}
			}
		}

		self.running.store(false, Ordering::Relaxed);

		if errors.is_empty() {
			println!(
				"[Subsystem] All subsystems stopped successfully"
			);
			Ok(())
		} else {
			let error_msg = format!(
				"Errors occurred while stopping {} subsystems: {:?}",
				errors.len(),
				errors
			);
			println!("[Subsystem] {}", error_msg);
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
		self.subsystems
			.iter()
			.map(|subsystem| subsystem.name().to_string())
			.collect()
	}

	pub fn get<T: 'static>(&self) -> Option<&T> {
		let type_id = TypeId::of::<T>();
		let index = *self.index.get(&type_id)?;
		self.subsystems.get(index)?.as_any().downcast_ref::<T>()
	}

	fn stop_started_subsystems(
		&mut self,
		started_names: &[String],
	) -> Result<()> {
		let mut errors = Vec::new();

		// Stop the started subsystems in reverse order
		for name in started_names.iter().rev() {
			// Find and stop the subsystem by name
			for subsystem in &mut self.subsystems {
				if subsystem.name() == name {
					if let Err(e) = subsystem.stop() {
						println!(
							"[Subsystem] Error stopping '{}' during rollback: {}",
							name, e
						);
						errors.push((name.clone(), e));
					}
					// Update health monitoring
					self.health_monitor
						.update_component_health(
							name.clone(),
							subsystem
								.health_status(
								),
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