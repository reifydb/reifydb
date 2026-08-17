// SPDX-License-Identifier: Apache-2.0
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
	subsystems: Vec<Arc<dyn Subsystem>>,
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

	/// Subsystems are shut down in reverse insertion order, so add order controls teardown order.
	pub fn add_subsystem(&mut self, subsystem: Box<dyn Subsystem>) {
		let subsystem: Arc<dyn Subsystem> = Arc::from(subsystem);

		self.health_monitor.register_subsystem(subsystem.name().to_string(), Arc::clone(&subsystem));

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
			info!("Successfully shut down: {}", name);
		}

		info!("All subsystems shut down");
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
