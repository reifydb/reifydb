// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace<T: AsTransaction>(
		&self,
		_rx: &mut T,
		create: logical::CreateNamespaceNode,
	) -> crate::Result<PhysicalPlan> {
		// FIXME validate catalog
		Ok(PhysicalPlan::CreateNamespace(CreateNamespaceNode {
			namespace: create.namespace,
			if_not_exists: create.if_not_exists,
		}))
	}
}
