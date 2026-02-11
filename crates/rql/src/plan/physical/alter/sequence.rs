// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::table_not_found,
	interface::resolved::{
		ResolvedColumn, ResolvedNamespace, ResolvedPrimitive, ResolvedSequence, ResolvedTable, SequenceDef,
	},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	nodes::AlterSequenceNode,
	plan::{
		logical::{self, resolver::DEFAULT_NAMESPACE},
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_sequence<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		alter: logical::AlterSequenceNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get the namespace name from the sequence identifier
		let namespace_name = if alter.sequence.namespace.is_empty() {
			DEFAULT_NAMESPACE.to_string()
		} else {
			alter.sequence.namespace.iter().map(|f| f.text()).collect::<Vec<_>>().join(".")
		};

		// Query the catalog for the actual namespace
		let namespace_def = self
			.catalog
			.find_namespace_by_name(rx, &namespace_name)?
			.unwrap_or_else(|| panic!("Namespace '{}' not found", namespace_name));

		// Query the catalog for the actual table
		let table_name = alter.sequence.name.text();
		let Some(table_def) = self.catalog.find_table_by_name(rx, namespace_def.id, table_name)? else {
			return_error!(table_not_found(
				self.interner.intern_fragment(&alter.sequence.name),
				&namespace_def.name,
				table_name
			));
		};

		// Find the column in the table
		let column_name = alter.column.name.text();
		let column_def = table_def
			.columns
			.iter()
			.find(|c| c.name == column_name)
			.unwrap_or_else(|| panic!("Column '{}' not found in table '{}'", column_name, table_name))
			.clone();

		// Create resolved namespace
		let namespace_fragment = Fragment::internal(namespace_def.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_fragment, namespace_def.clone());

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
		let resolved_table = ResolvedTable::new(table_fragment, resolved_namespace, table_def);

		// Create resolved source and column
		let resolved_source = ResolvedPrimitive::Table(resolved_table);
		let resolved_column = ResolvedColumn::new(
			self.interner.intern_fragment(&alter.column.name),
			resolved_source,
			column_def,
		);

		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: resolved_sequence,
			column: resolved_column,
			value: alter.value,
		}))
	}
}
