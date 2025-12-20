// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use FlowNodeType::Window;
use reifydb_core::{
	WindowSize, WindowSlide, WindowType,
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
	plan::physical::{PhysicalPlan, WindowNode},
};

pub(crate) struct WindowCompiler {
	pub input: Option<Box<PhysicalPlan<'static>>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'static>>,
	pub aggregations: Vec<Expression<'static>>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

impl<'a> From<WindowNode<'a>> for WindowCompiler {
	fn from(node: WindowNode<'a>) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			window_type: node.window_type,
			size: node.size,
			slide: node.slide,
			group_by: to_owned_expressions(node.group_by),
			aggregations: to_owned_expressions(node.aggregations),
			min_events: node.min_events,
			max_window_count: node.max_window_count,
			max_window_age: node.max_window_age,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for WindowCompiler {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		// Compile input first if present
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(*input).await?)
		} else {
			None
		};

		// Now create the builder
		let mut builder = compiler.build_node(Window {
			window_type: self.window_type,
			size: self.size,
			slide: self.slide,
			group_by: self.group_by,
			aggregations: self.aggregations,
			min_events: self.min_events,
			max_window_count: self.max_window_count,
			max_window_age: self.max_window_age,
		});

		// Add input if we have one
		if let Some(input_node) = input_node {
			builder = builder.with_input(input_node);
		}

		builder.build().await
	}
}
