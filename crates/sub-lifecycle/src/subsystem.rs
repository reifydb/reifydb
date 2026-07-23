// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	lifecycle::class::RetentionClass,
};
use reifydb_runtime::{actor::mailbox::ActorRef, shutdown::Shutdown};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::{debug, info};

use crate::actor::LifecycleMessage;

pub struct LifecycleSubsystem {
	actor_ref: ActorRef<LifecycleMessage>,
	task_names: Vec<&'static str>,
	running: Arc<AtomicBool>,
}

fn report_policies(task_names: &[&'static str]) {
	info!(classes = task_names.len(), tasks = ?task_names, "Lifecycle subsystem started");
	for class in RetentionClass::all() {
		let terms: Vec<String> = class.floor_terms().iter().map(|term| term.to_string()).collect();
		info!(class = class.name(), floor = ?terms, "lifecycle retention policy");
	}
}

impl LifecycleSubsystem {
	pub fn new(actor_ref: ActorRef<LifecycleMessage>, task_names: Vec<&'static str>) -> Self {
		report_policies(&task_names);
		Self {
			actor_ref,
			task_names,
			running: Arc::new(AtomicBool::new(true)),
		}
	}

	pub fn task_names(&self) -> &[&'static str] {
		&self.task_names
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
		"Lifecycle"
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
