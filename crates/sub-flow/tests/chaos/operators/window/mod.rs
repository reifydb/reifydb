// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod count;
pub mod grid;
pub mod rolling;
pub mod session;
pub mod sliding;
pub mod tumbling;

use std::sync::Arc;

use reifydb_core::{common::WindowKind, interface::catalog::flow::OperatorId};
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sub_flow::{
	context::FlowContext,
	operator::{
		OperatorCell,
		scan::series::SourceSeriesOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};
use reifydb_value::value::duration::Duration;

pub struct WindowSpec {
	pub kind: WindowKind,
	pub group_by: &'static str,
	pub aggregations: &'static str,
	pub grace: Duration,
}

use crate::operators::routines;

pub fn build(spec: &WindowSpec, runtime: RuntimeContext) -> WindowOperator {
	let operator = OperatorId(1);
	let parent = OperatorCell::new(SourceSeriesOperator::new(OperatorId(0)));

	WindowOperator::new(WindowConfig {
		parent,
		operator,
		kind: spec.kind.clone(),
		group_by: parse_expression(spec.group_by).expect("group_by parses"),
		aggregations: parse_expression(spec.aggregations).expect("aggregations parse"),
		runtime_context: runtime,
		routines: routines(),
		grace: spec.grace,
		ctx: Arc::new(FlowContext::default()),
	})
}
