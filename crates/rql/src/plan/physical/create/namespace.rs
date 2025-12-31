// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::IntoStandardTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace<T: IntoStandardTransaction>(
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
