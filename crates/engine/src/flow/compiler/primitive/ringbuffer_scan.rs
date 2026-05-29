// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceRingBuffer, nodes::RingBufferScanNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct RingBufferScanCompiler {
	pub ringbuffer_scan: RingBufferScanNode,
}

impl From<RingBufferScanNode> for RingBufferScanCompiler {
	fn from(ringbuffer_scan: RingBufferScanNode) -> Self {
		Self {
			ringbuffer_scan,
		}
	}
}

impl CompileOperator for RingBufferScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let ringbuffer_id = self.ringbuffer_scan.source.def().id;
		compiler.add_node(
			txn,
			SourceRingBuffer {
				ringbuffer: ringbuffer_id,
			},
		)
	}
}
