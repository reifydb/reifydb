// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorTtlChangeOperations, flow::FlowNodeId},
	row::Ttl,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalOperatorTtlChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackOperatorTtlChangeOperations for AdminTransaction {
	fn track_operator_ttl_created(&mut self, node: FlowNodeId, ttl: Ttl) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some((node, ttl)),
			op: Create,
		};
		self.changes.add_operator_ttl_change(change);
		Ok(())
	}

	fn track_operator_ttl_updated(&mut self, node: FlowNodeId, pre: Ttl, post: Ttl) -> Result<()> {
		let change = Change {
			pre: Some((node, pre)),
			post: Some((node, post)),
			op: Update,
		};
		self.changes.add_operator_ttl_change(change);
		Ok(())
	}

	fn track_operator_ttl_deleted(&mut self, node: FlowNodeId, ttl: Ttl) -> Result<()> {
		let change = Change {
			pre: Some((node, ttl)),
			post: None,
			op: Delete,
		};
		self.changes.add_operator_ttl_change(change);
		Ok(())
	}
}

impl TransactionalOperatorTtlChanges for AdminTransaction {
	fn find_operator_ttl(&self, node: FlowNodeId) -> Option<&Ttl> {
		for change in self.changes.operator_ttl.iter().rev() {
			if let Some((n, ttl)) = &change.post {
				if *n == node {
					return Some(ttl);
				}
			} else if let Some((n, _)) = &change.pre
				&& *n == node && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_operator_ttl_deleted(&self, node: FlowNodeId) -> bool {
		self.changes.operator_ttl.iter().rev().any(|change| {
			change.op == Delete && change.pre.as_ref().map(|(n, _)| *n == node).unwrap_or(false)
		})
	}
}
