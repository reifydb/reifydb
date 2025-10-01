// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::interface::{
	QueryTransaction,
	resolved::{ResolvedColumn, ResolvedNamespace, ResolvedSequence, ResolvedSource, ResolvedTable, SequenceDef},
};
use reifydb_type::{Fragment, diagnostic::catalog::table_not_found, return_error};

use crate::plan::{
	logical::{self, resolver::DEFAULT_NAMESPACE},
	physical::{AlterSequenceNode, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		rx: &mut impl QueryTransaction,
		alter: logical::AlterSequenceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Get the namespace name from the sequence identifier
		let namespace_name = alter.sequence.namespace.as_ref().map(|f| f.text()).unwrap_or(DEFAULT_NAMESPACE);

		// Query the catalog for the actual namespace
		let namespace_def = CatalogStore::find_namespace_by_name(rx, namespace_name)?
			.unwrap_or_else(|| panic!("Namespace '{}' not found", namespace_name));

		// Query the catalog for the actual table
		let table_name = alter.sequence.name.text();
		let Some(table_def) = CatalogStore::find_table_by_name(rx, namespace_def.id, table_name)? else {
			return_error!(table_not_found(
				alter.sequence.name.clone().into_owned(),
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
		let namespace_fragment = Fragment::owned_internal(namespace_def.name.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_fragment, namespace_def.clone());

		// Create resolved sequence (using table name as sequence name)
		let sequence_def = SequenceDef {
			name: table_name.to_string(),
			current_value: 1, // This is not used in ALTER SEQUENCE, just a placeholder
			increment: 1,
		};
		let resolved_sequence =
			ResolvedSequence::new(alter.sequence.name.clone(), resolved_namespace.clone(), sequence_def);

		// Create resolved table
		let table_fragment = Fragment::owned_internal(table_name.to_string());
		let resolved_table = ResolvedTable::new(table_fragment, resolved_namespace, table_def);

		// Create resolved source and column
		let resolved_source = ResolvedSource::Table(resolved_table);
		let resolved_column = ResolvedColumn::new(alter.column.name.clone(), resolved_source, column_def);

		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: resolved_sequence,
			column: resolved_column,
			value: alter.value,
		}))
	}
}
