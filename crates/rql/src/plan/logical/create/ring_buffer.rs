// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, store::ring_buffer::create::RingBufferColumnToCreate};
use reifydb_core::interface::ColumnPolicyKind;
use reifydb_type::Fragment;

use crate::{
	ast::AstCreateRingBuffer,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateRingBufferNode, LogicalPlan, convert_policy},
};

impl Compiler {
	pub(crate) fn compile_create_ring_buffer<'a, T: CatalogQueryTransaction>(
		ast: AstCreateRingBuffer<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<RingBufferColumnToCreate> = vec![];

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = convert_data_type_with_constraints(&col.ty)?;

			let policies = if let Some(policy_block) = &col.policies {
				policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
			} else {
				vec![]
			};

			let ty_fragment = match &col.ty {
				crate::ast::AstDataType::Unconstrained(fragment) => fragment.clone(),
				crate::ast::AstDataType::Constrained {
					name,
					..
				} => name.clone(),
			};

			let fragment = Some(Fragment::merge_all([col.name.clone(), ty_fragment]).into_owned());

			columns.push(RingBufferColumnToCreate {
				name: column_name,
				constraint,
				policies,
				auto_increment: col.auto_increment,
				fragment,
			});
		}

		// Use the ring buffer identifier directly from AST
		let ring_buffer = ast.ring_buffer;

		Ok(LogicalPlan::CreateRingBuffer(CreateRingBufferNode {
			ring_buffer,
			if_not_exists: false,
			columns,
			capacity: ast.capacity,
		}))
	}
}
