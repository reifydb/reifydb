// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of series scan operations

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceSeries, nodes::SeriesScanNode};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		let series_id = self.series_scan.source.def().id;
		compiler.add_node(
			txn,
			SourceSeries {
				series: series_id,
			},
		)
	}
}
