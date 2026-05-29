// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceSeries, nodes::SeriesScanNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct SeriesScanCompiler {
	pub series_scan: SeriesScanNode,
}

impl From<SeriesScanNode> for SeriesScanCompiler {
	fn from(series_scan: SeriesScanNode) -> Self {
		Self {
			series_scan,
		}
	}
}

impl CompileOperator for SeriesScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let series_id = self.series_scan.source.def().id;
		compiler.add_node(
			txn,
			SourceSeries {
				series: series_id,
			},
		)
	}
}
