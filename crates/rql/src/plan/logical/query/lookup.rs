// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::operation::{lookup_block_not_bare_from, lookup_using_not_partition};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error;

use crate::{
	Result,
	ast::ast::{AstLookup, JoinConnector},
	plan::logical::{
		Compiler, LogicalPlan, LookupNode, ObjectScanNode, RemoteScanNode, query::join::build_join_expressions,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_lookup(
		&self,
		ast: AstLookup<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let AstLookup {
			token,
			join_type,
			subquery,
			using_clause,
			alias,
			with,
			..
		} = ast;

		let block = subquery.token.fragment.to_owned();
		if subquery.statement.nodes.is_empty() {
			return Err(error!(lookup_block_not_bare_from(block)));
		}
		let subquery = self.compile_join_subquery_nodes(subquery, &alias, tx)?;
		let right = match subquery.as_slice() {
			[
				LogicalPlan::SourceScan(ObjectScanNode {
					source,
					..
				}),
			] => source.fully_qualified_name().unwrap_or_else(|| source.identifier().text().to_string()),
			[
				LogicalPlan::RemoteScan(RemoteScanNode {
					remote_name,
					..
				}),
			] => remote_name.clone(),
			_ => return Err(error!(lookup_block_not_bare_from(block))),
		};

		if using_clause.pairs.iter().any(|pair| matches!(pair.connector, Some(JoinConnector::Or))) {
			return Err(error!(lookup_using_not_partition(using_clause.token.fragment.to_owned(), &right)));
		}
		let on = build_join_expressions(using_clause, &alias)?;
		let with = Self::compile_lookup_with(with.as_ref(), token.fragment.to_owned())?;

		Ok(LogicalPlan::Lookup(LookupNode {
			join_type,
			subquery,
			on,
			alias,
			with,
		}))
	}
}
