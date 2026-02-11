// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_namespace<T: AsTransaction>(
		&mut self,
		_rx: &mut T,
		create: logical::CreateNamespaceNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		Ok(PhysicalPlan::CreateNamespace(CreateNamespaceNode {
			segments: create.segments.iter().map(|s| self.interner.intern_fragment(s)).collect(),
			if_not_exists: create.if_not_exists,
		}))
	}
}
