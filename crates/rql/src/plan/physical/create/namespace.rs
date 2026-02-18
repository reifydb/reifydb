// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace(
		&mut self,
		_rx: &mut Transaction<'_>,
		create: logical::CreateNamespaceNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		Ok(PhysicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: create.segments.iter().map(|s| self.interner.intern_fragment(s)).collect(),
			if_not_exists: create.if_not_exists,
		}))
	}
}
