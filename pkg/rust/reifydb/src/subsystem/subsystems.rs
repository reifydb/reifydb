// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::TypeId,
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_sub_api::subsystem::Subsystem;
use tracing::info;

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
			running: Arc::new(AtomicBool::new(true)),
			health_monitor,
		}
	}

	/// Add a born-running subsystem to be managed. Subsystems are shut down in
	/// reverse insertion order, so callers control teardown order by add order.
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

	pub fn shutdown_all(&self) {
		if self.running.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed).is_err() {
			return;
		}

		info!("Shutting down {} subsystems...", self.subsystems.len());

		for subsystem in self.subsystems.iter().rev() {
			let name = subsystem.name();
			info!("Shutting down subsystem: {}", name);
			subsystem.shutdown();
			self.health_monitor.update_component_health(
				name.to_string(),
				subsystem.health_status(),
				subsystem.is_running(),
			);
			info!("Successfully shut down: {}", name);
		}

		info!("All subsystems shut down");
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
}

impl Drop for Subsystems {
	fn drop(&mut self) {
		self.shutdown_all();
	}
}
