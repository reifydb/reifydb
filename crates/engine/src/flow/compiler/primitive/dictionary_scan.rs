// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{flow::node::FlowNodeType::SourceDictionary, nodes::DictionaryScanNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct DictionaryScanCompiler {
	pub dictionary_scan: DictionaryScanNode,
}

impl From<DictionaryScanNode> for DictionaryScanCompiler {
	fn from(dictionary_scan: DictionaryScanNode) -> Self {
		Self {
			dictionary_scan,
		}
	}
}

impl CompileOperator for DictionaryScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		let dictionary_id = self.dictionary_scan.source.def().id;
		compiler.add_node(
			txn,
			SourceDictionary {
				dictionary: dictionary_id,
			},
		)
	}
}
