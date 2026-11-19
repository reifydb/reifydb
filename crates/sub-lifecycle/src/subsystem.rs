// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::HashSet,
	mem::take,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	lifecycle::{class::LifecycleClass, coverage::LifecycleCoverage},
};
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorHandle},
	shutdown::{Shutdown, ShutdownErrors},
	sync::mutex::Mutex,
};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::error::ValueError;
use tracing::{debug, error, info};

use crate::{actor::LifecycleMessage, plane::LifecyclePlane};

pub struct LifecycleSubsystem {
	actor_refs: Vec<ActorRef<LifecycleMessage>>,
	handles: Mutex<Vec<ActorHandle<LifecycleMessage>>>,
	task_names: Vec<&'static str>,
	covered: HashSet<LifecycleClass>,
	plane: LifecyclePlane,
	running: Arc<AtomicBool>,
}

fn report_lifecycle_classes(task_names: &[&'static str], coverage: &LifecycleCoverage) {
	info!(tasks = task_names.len(), names = ?task_names, "Lifecycle subsystem started");
	for class in LifecycleClass::all() {
		let terms: Vec<String> = class.floor_terms().iter().map(|term| term.to_string()).collect();
		match (coverage.owner(*class), coverage.absence(*class)) {
			(Some(owner), _) => {
				info!(class = class.name(), owner, floor = ?terms, "lifecycle class")
			}
			(None, Some(reason)) => info!(
				class = class.name(),
				reason, "lifecycle class has no producer here; nothing can accumulate in it"
			),
			(None, None) => error!(
				class = class.name(),
				floor = ?terms,
				"lifecycle class has NO registered executor; nothing reclaims it"
			),
		}
	}
}

impl LifecycleSubsystem {
	pub fn new(
		handles: Vec<ActorHandle<LifecycleMessage>>,
		task_names: Vec<&'static str>,
		coverage: LifecycleCoverage,
		plane: LifecyclePlane,
	) -> Self {
		report_lifecycle_classes(&task_names, &coverage);
		let covered = LifecycleClass::all().iter().filter(|c| coverage.is_covered(**c)).copied().collect();
		Self {
			actor_refs: handles.iter().map(|handle| handle.actor_ref().clone()).collect(),
			handles: Mutex::new(handles),
			task_names,
			covered,
			plane,
			running: Arc::new(AtomicBool::new(true)),
		}
	}

	pub fn task_names(&self) -> &[&'static str] {
		&self.task_names
	}

	pub fn covered_classes(&self) -> &HashSet<LifecycleClass> {
		&self.covered
	}

	pub fn plane(&self) -> &LifecyclePlane {
		&self.plane
	}

	pub fn actor_ref(&self, index: usize) -> Option<&ActorRef<LifecycleMessage>> {
		self.actor_refs.get(index)
	}
}

impl HasVersion for LifecycleSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-lifecycle".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Data lifecycle subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for LifecycleSubsystem {
	fn shutdown(&self) -> Result<(), ValueError> {
		if !self.running.swap(false, Ordering::SeqCst) {
			return Ok(());
		}
		for actor_ref in &self.actor_refs {
			let _ = actor_ref.send_blocking(LifecycleMessage::Shutdown);
		}
		let mut errors = ShutdownErrors::new("lifecycle");
		for handle in take(&mut *self.handles.lock()) {
			if let Err(err) = handle.join() {
				errors.fail("actor", err);
			}
		}
		debug!("Lifecycle subsystem stopped");
		errors.finish()
	}
}

impl Subsystem for LifecycleSubsystem {
	fn name(&self) -> &'static str {
		"lifecycle"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Lifecycle subsystem not running".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
