// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::HashSet,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	lifecycle::{class::RetentionClass, coverage::RetentionCoverage},
};
use reifydb_runtime::{actor::mailbox::ActorRef, shutdown::Shutdown};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::{debug, error, info};

use crate::{actor::LifecycleMessage, plane::RetentionPlane};

pub struct LifecycleSubsystem {
	actor_ref: ActorRef<LifecycleMessage>,
	task_names: Vec<&'static str>,
	covered: HashSet<RetentionClass>,
	plane: RetentionPlane,
	running: Arc<AtomicBool>,
}

fn report_retention_classes(task_names: &[&'static str], coverage: &RetentionCoverage) {
	info!(tasks = task_names.len(), names = ?task_names, "Lifecycle subsystem started");
	for class in RetentionClass::all() {
		let terms: Vec<String> = class.floor_terms().iter().map(|term| term.to_string()).collect();
		match (coverage.owner(*class), coverage.absence(*class)) {
			(Some(owner), _) => {
				info!(class = class.name(), owner, floor = ?terms, "lifecycle retention class")
			}
			(None, Some(reason)) => info!(
				class = class.name(),
				reason, "lifecycle retention class has no producer here; nothing can accumulate in it"
			),
			(None, None) => error!(
				class = class.name(),
				floor = ?terms,
				"lifecycle retention class has NO registered executor; nothing reclaims it"
			),
		}
	}
}

impl LifecycleSubsystem {
	pub fn new(
		actor_ref: ActorRef<LifecycleMessage>,
		task_names: Vec<&'static str>,
		coverage: RetentionCoverage,
		plane: RetentionPlane,
	) -> Self {
		report_retention_classes(&task_names, &coverage);
		let covered = RetentionClass::all().iter().filter(|c| coverage.is_covered(**c)).copied().collect();
		Self {
			actor_ref,
			task_names,
			covered,
			plane,
			running: Arc::new(AtomicBool::new(true)),
		}
	}

	pub fn task_names(&self) -> &[&'static str] {
		&self.task_names
	}

	pub fn covered_classes(&self) -> &HashSet<RetentionClass> {
		&self.covered
	}

	pub fn plane(&self) -> &RetentionPlane {
		&self.plane
	}

	pub fn actor_ref(&self) -> &ActorRef<LifecycleMessage> {
		&self.actor_ref
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
	fn shutdown(&self) {
		if !self.running.swap(false, Ordering::SeqCst) {
			return;
		}
		let _ = self.actor_ref.send(LifecycleMessage::Shutdown);
		debug!("Lifecycle subsystem shutdown signalled");
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
