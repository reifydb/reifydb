// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Subsystem contract: always-on reporting, and a shutdown that is both effective and idempotent.
//!
//! The health surface answers "is reclamation running?", so a subsystem reporting Healthy while its lane is
//! stopped is worse than one reporting nothing at all.

use reifydb_core::{
	interface::version::ComponentType,
	lifecycle::{metrics::RetentionMetrics, registry::LifecycleRegistry},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_lifecycle::{factory::LifecycleSubsystemFactory, subsystem::LifecycleSubsystem};
use reifydb_test_harness::engine::TestEngine;

fn boot() -> (TestEngine, Box<dyn Subsystem>) {
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());
	let subsystem =
		Box::new(LifecycleSubsystemFactory).create(&ioc).expect("the lifecycle subsystem must always start");
	(test_engine, subsystem)
}

#[test]
fn reports_running_and_healthy_from_the_moment_it_is_created() {
	// "Always on" is the subsystem's whole premise: there is no configuration under which it starts inert.
	let (_engine, subsystem) = boot();

	assert!(subsystem.is_running(), "the lifecycle subsystem must be running immediately after creation");
	assert!(
		matches!(subsystem.health_status(), HealthStatus::Healthy),
		"a freshly created lifecycle subsystem must report Healthy"
	);
	assert_eq!(subsystem.name(), "lifecycle");
}

#[test]
fn stops_reporting_healthy_once_shut_down() {
	// The failure mode this guards against: a stopped lane still advertising Healthy, so nothing alarms while
	// reclamation has silently ceased and the database grows without bound.
	let (_engine, subsystem) = boot();

	subsystem.shutdown();

	assert!(!subsystem.is_running(), "shutdown must clear the running flag");
	assert!(
		matches!(subsystem.health_status(), HealthStatus::Failed { .. }),
		"a shut-down lifecycle subsystem must NOT report Healthy - that is the misleading-signal failure"
	);
}

#[test]
fn shutdown_is_idempotent() {
	// Subsystems are stopped in reverse registration order and may also be dropped; a second shutdown must not
	// re-send Stop to an already-dead lane or panic during teardown.
	let (_engine, subsystem) = boot();

	subsystem.shutdown();
	subsystem.shutdown();

	assert!(!subsystem.is_running(), "repeated shutdown must remain stopped");
}

#[test]
fn reports_the_classes_it_owns_so_the_boot_report_can_be_audited() {
	// A class that fails to register is invisible unless the subsystem can be asked what it took ownership of.
	// This report is what makes "which classes are live on this deployment?" answerable at all.
	let (_engine, subsystem) = boot();
	let lifecycle = subsystem
		.as_any()
		.downcast_ref::<LifecycleSubsystem>()
		.expect("the factory must produce a LifecycleSubsystem");

	assert!(!lifecycle.task_names().is_empty(), "the subsystem must report the classes it owns");
	assert!(
		lifecycle.task_names().iter().all(|name| !name.is_empty()),
		"every reported class needs a usable name: {:?}",
		lifecycle.task_names()
	);
}

#[test]
fn declares_itself_as_a_subsystem_component_for_the_version_catalog() {
	let (_engine, subsystem) = boot();
	let version = subsystem.version();

	assert_eq!(version.name, "sub-lifecycle");
	assert!(
		matches!(version.r#type, ComponentType::Subsystem),
		"the version catalog groups components by type; a mislabelled entry hides the subsystem from it"
	);
	assert!(!version.version.is_empty(), "the reported version must not be empty");
}
