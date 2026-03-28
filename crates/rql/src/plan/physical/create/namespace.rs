// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	nodes::CreateRemoteNamespaceNode,
	plan::{
		logical,
		physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace(
		&mut self,
		_rx: &mut Transaction<'_>,
		create: logical::CreateNamespaceNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		Ok(PhysicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: create.segments.iter().map(|s| self.interner.intern_fragment(s)).collect(),
			if_not_exists: create.if_not_exists,
		}))
	}

	pub(crate) fn compile_create_remote_namespace(
		&mut self,
		_rx: &mut Transaction<'_>,
		create: logical::CreateRemoteNamespaceNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		Ok(PhysicalPlan::CreateRemoteNamespace(CreateRemoteNamespaceNode {
			segments: create.segments.iter().map(|s| self.interner.intern_fragment(s)).collect(),
			if_not_exists: create.if_not_exists,
			grpc: self.interner.intern_fragment(&create.grpc),
			token: create.token.as_ref().map(|t| self.interner.intern_fragment(t)),
		}))
	}
}
