// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::table_not_found,
	interface::resolved::{
		ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedSequence, ResolvedTable, SequenceDef,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	nodes::AlterSequenceNode,
	plan::{
		logical::{self},
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_sequence(
		&mut self,
		rx: &mut Transaction<'_>,
		alter: logical::AlterSequenceNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Get the namespace segments from the sequence identifier
		let ns_segments: Vec<&str> = alter.sequence.namespace.iter().map(|n| n.text()).collect();

		// Query the catalog for the actual namespace
		let namespace = self
			.catalog
			.find_namespace_by_segments(rx, &ns_segments)?
			.unwrap_or_else(|| panic!("Namespace '{}' not found", ns_segments.join("::")));

		// Query the catalog for the actual table
		let table_name = alter.sequence.name.text();
		let Some(table) = self.catalog.find_table_by_name(rx, namespace.id(), table_name)? else {
			return_error!(table_not_found(
				self.interner.intern_fragment(&alter.sequence.name),
				namespace.name(),
				table_name
			));
		};

		// Find the column in the table
		let column_name = alter.column.name.text();
		let column = table
			.columns
			.iter()
			.find(|c| c.name == column_name)
			.unwrap_or_else(|| panic!("Column '{}' not found in table '{}'", column_name, table_name))
			.clone();

		// Create resolved namespace
		let namespace_fragment = Fragment::internal(namespace.name().to_string());
		let resolved_namespace = ResolvedNamespace::new(namespace_fragment, namespace.clone());

		// Create resolved sequence (using table name as sequence name)
		let sequence_def = SequenceDef {
			name: table_name.to_string(),
			current_value: 1, // This is not used in ALTER SEQUENCE, just a placeholder
			increment: 1,
		};
		let resolved_sequence = ResolvedSequence::new(
			self.interner.intern_fragment(&alter.sequence.name),
			resolved_namespace.clone(),
			sequence_def,
		);

		// Create resolved table
		let table_fragment = Fragment::internal(table_name.to_string());
		let resolved_table = ResolvedTable::new(table_fragment, resolved_namespace, table);

		// Create resolved source and column
		let resolved_source = ResolvedPrimitive::Table(resolved_table);
		let resolved_column =
			ResolvedColumn::new(self.interner.intern_fragment(&alter.column.name), resolved_source, column);

		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: resolved_sequence,
			column: resolved_column,
			value: alter.value,
		}))
	}
}
