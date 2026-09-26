// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc, thread::sleep};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::{
	common::{OperatorClass, WindowRequirements, WindowSizeDomain},
	interface::{
		catalog::flow::OperatorId,
		change::Change,
		flow::{OperatorCapability, to_bitmask},
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow_async::operator::{BoxedHostOperator, HostOperator, host::HostContext};
use reifydb_runtime::{RuntimeConfig, context::clock::Clock, fatal::FatalConfig};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	builder::{CustomOperatorEntry, CustomOperators, FlowConfig},
	subsystem::FlowSubsystem,
};

const SETTLE: Duration = Duration::from_seconds_const(5);

const PANICKING_OPERATOR: &str = "panics_on_apply";

struct PanicsOnApply {
	operator: OperatorId,
}

impl HostOperator for PanicsOnApply {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, _host: &mut dyn HostContext, _change: Change) -> Result<Change> {
		panic!("{PANICKING_OPERATOR} fails every flow step it is given")
	}
}

struct FlowWithAPanickingOperator;

impl SubsystemFactory for FlowWithAPanickingOperator {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let entry = CustomOperatorEntry {
			factory: Arc::new(|operator, _params, _with| {
				Ok(Box::new(PanicsOnApply {
					operator,
				}) as BoxedHostOperator)
			}),
			abi: None,
			version: "0.0.0".to_string(),
			description: "a native operator whose apply panics".to_string(),
			capabilities: to_bitmask(OperatorCapability::STANDARD),
			input: Vec::new(),
			output: Vec::new(),
			class: OperatorClass::Unmanaged,
			window: WindowRequirements {
				takes_window: false,
				kinds: &[],
				domain: WindowSizeDomain::Time,
				needs_pane: false,
				throttles: false,
			},
			unmanaged_because: None,
		};
		let config = FlowConfig {
			operators_dir: None,
			custom_operators: CustomOperators::new(HashMap::from([(
				PANICKING_OPERATOR.to_string(),
				entry,
			)])),
		};
		FlowSubsystem::publish_operator_catalog(&config, &engine);
		Ok(Box::new(FlowSubsystem::new(config, engine, ioc)?))
	}
}

fn description(status: &HealthStatus) -> Option<&str> {
	match status {
		HealthStatus::Warning {
			description,
		}
		| HealthStatus::Degraded {
			description,
		}
		| HealthStatus::Failed {
			description,
		} => Some(description),
		HealthStatus::Healthy | HealthStatus::Unknown => None,
	}
}

#[test]
fn a_flow_step_that_panics_leaves_the_flow_subsystem_not_healthy_and_names_the_failure() {
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_subsystem(Box::new(FlowWithAPanickingOperator))
			.build()
			.expect("build memory db with a flow subsystem holding a panicking operator"),
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { a: int4 }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::v {{ a: int4 }} AS {{ FROM app::t APPLY {PANICKING_OPERATOR}{{}} }}"
	));
	db.command("INSERT app::t [{ a: 1 }]");

	let deadline = Clock::Real.instant() + SETTLE;
	let status = loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if description(&status).is_some() || Clock::Real.instant() >= deadline {
			break status;
		}
		sleep(Duration::from_milliseconds_const(20).to_std());
	};

	let Some(description) = description(&status) else {
		panic!("the flow step panicked in {PANICKING_OPERATOR}, so flow health must not stay {status:?}");
	};
	assert!(
		description.contains(PANICKING_OPERATOR),
		"the health report must name the failed step, got: {description}"
	);
}

#[test]
fn a_registered_custom_operator_is_in_the_operator_catalog_before_any_view_is_created() {
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_subsystem(Box::new(FlowWithAPanickingOperator))
			.build()
			.expect("build memory db with a flow subsystem holding a panicking operator"),
	);

	assert_eq!(
		db.row_count(&format!(
			"FROM system::operator_libraries FILTER {{ operator == '{PANICKING_OPERATOR}' }}"
		)),
		1,
		"the operator must be published by the time the first statement runs"
	);
}
