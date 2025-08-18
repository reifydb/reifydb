// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::collections::HashMap;

use reifydb_core::interface::{Evaluator, FlowId, FlowNodeId, SourceId};

use crate::{
	Flow,
	operator::{Operator, OperatorContext},
};

pub struct FlowEngine<'a, E: Evaluator> {
	evaluator: E,
	operators: HashMap<FlowNodeId, Box<dyn Operator<E>>>,
	contexts: HashMap<FlowNodeId, OperatorContext<'a, E>>,
	flows: HashMap<FlowId, Flow>,
	sources: HashMap<SourceId, Vec<FlowId>>,
	sinks: HashMap<SourceId, Vec<FlowId>>,
}

impl<E: Evaluator> FlowEngine<'_, E> {
	pub fn new(evaluator: E) -> Self {
		Self {
			evaluator,
			operators: HashMap::new(),
			contexts: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
		}
	}
}
