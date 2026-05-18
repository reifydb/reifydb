// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::namespace_not_found,
	interface::catalog::procedure::{ProcedureParam, RqlTrigger},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result, convert_data_type_with_constraints, nodes,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_procedure(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateProcedureNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		if create.on_event.is_some() {
			return self.compile_create_handler(rx, create);
		}

		let ns_segments: Vec<&str> = create.procedure.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.procedure.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let mut params = Vec::with_capacity(create.params.len());
		for param in &create.params {
			let constraint = convert_data_type_with_constraints(&param.param_type)?;
			params.push(ProcedureParam {
				name: param.name.text().to_string(),
				param_type: constraint,
			});
		}

		Ok(PhysicalPlan::CreateProcedure(nodes::CreateProcedureNode {
			namespace,
			name: self.interner.intern_fragment(&create.procedure.name),
			params,
			body_source: create.body_source,
			trigger: RqlTrigger::Call,
			is_test: create.is_test,
		}))
	}
}
