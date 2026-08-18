// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_rql::flow::analyzer::FlowDependencyGraph;

#[derive(Debug, Clone)]
pub struct FlowSchedule {
	pub roots: Vec<FlowId>,
	pub consumers: BTreeMap<FlowId, Vec<FlowId>>,
	pub in_degree: BTreeMap<FlowId, usize>,
}

pub(crate) fn calculate_schedule(dependency_graph: &FlowDependencyGraph) -> FlowSchedule {
	let mut in_degree: BTreeMap<FlowId, usize> = BTreeMap::new();
	let mut consumers: BTreeMap<FlowId, Vec<FlowId>> = BTreeMap::new();

	for flow_summary in &dependency_graph.flows {
		in_degree.insert(flow_summary.id, 0);
		consumers.insert(flow_summary.id, Vec::new());
	}

	for dependency in &dependency_graph.dependencies {
		consumers.entry(dependency.source_flow).or_default().push(dependency.target_flow);
		*in_degree.entry(dependency.target_flow).or_default() += 1;
	}

	let roots = in_degree.iter().filter(|&(_, deg)| *deg == 0).map(|(id, _)| *id).collect();

	FlowSchedule {
		roots,
		consumers,
		in_degree,
	}
}
