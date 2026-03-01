// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::namespace_not_found,
	interface::catalog::procedure::{ProcedureParamDef, ProcedureTrigger},
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
		// If this is a handler-style procedure (has on_event), delegate to handler compiler
		if create.on_event.is_some() {
			return self.compile_create_handler(rx, create);
		}

		// Resolve namespace
		let namespace_name = if create.procedure.namespace.is_empty() {
			"default".to_string()
		} else {
			create.procedure.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join("::")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.procedure.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		// Convert params
		let mut params = Vec::with_capacity(create.params.len());
		for param in &create.params {
			let constraint = convert_data_type_with_constraints(&param.param_type)?;
			params.push(ProcedureParamDef {
				name: param.name.text().to_string(),
				param_type: constraint,
			});
		}

		Ok(PhysicalPlan::CreateProcedure(nodes::CreateProcedureNode {
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.procedure.name),
			params,
			body_source: create.body_source,
			trigger: ProcedureTrigger::Call,
		}))
	}
}
