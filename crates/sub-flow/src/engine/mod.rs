// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::collections::HashMap;

use reifydb_core::{
	flow::Flow,
	interface::{Evaluator, FlowId, FlowNodeId, SourceId},
};

use crate::operator::OperatorEnum;

pub struct FlowEngine<E: Evaluator> {
	evaluator: E,
	operators: HashMap<FlowNodeId, OperatorEnum<E>>,
	flows: HashMap<FlowId, Flow<'static>>,
	sources: HashMap<SourceId, Vec<FlowId>>,
	sinks: HashMap<SourceId, Vec<FlowId>>,
}

impl<E: Evaluator> FlowEngine<E> {
	pub fn new(evaluator: E) -> Self {
		Self {
			evaluator,
			operators: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
		}
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.flows.is_empty()
	}
}
