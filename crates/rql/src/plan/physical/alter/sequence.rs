// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ColumnDef, ColumnId, NamespaceDef, NamespaceId, QueryTransaction, TableDef, TableId,
	catalog::ColumnIndex,
	resolved::{ResolvedColumn, ResolvedNamespace, ResolvedSequence, ResolvedSource, ResolvedTable, SequenceDef},
};
use reifydb_type::{Fragment, Type, TypeConstraint};

use crate::plan::{
	logical::{self, resolver::DEFAULT_NAMESPACE},
	physical::{AlterSequenceNode, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		_rx: &mut impl QueryTransaction,
		alter: logical::AlterSequenceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Resolve the sequence identifier to a ResolvedSequence
		// For now, create a basic resolved sequence (in a real implementation,
		// this would query the catalog to get the actual sequence definition)
		let namespace_name = alter.sequence.namespace.as_ref().map(|f| f.text()).unwrap_or(DEFAULT_NAMESPACE);

		let namespace = ResolvedNamespace::new(
			Fragment::owned_internal(namespace_name),
			NamespaceDef {
				id: NamespaceId(1), // In real implementation, this would come from catalog
				name: namespace_name.to_string(),
			},
		);

		let sequence_def = SequenceDef {
			name: alter.sequence.name.text().to_string(),
			current_value: 1, // In real implementation, this would come from catalog
			increment: 1,
		};

		let resolved_sequence =
			ResolvedSequence::new(alter.sequence.name.clone(), namespace.clone(), sequence_def);

		// Create a resolved table (in real implementation, this would come from catalog lookup)
		let table_def = TableDef {
			id: TableId(1),
			namespace: NamespaceId(1),
			name: alter.sequence.name.text().to_string(),
			columns: vec![ColumnDef {
				id: ColumnId(1),
				name: alter.column.name.text().to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int8), // Placeholder
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			}],
			primary_key: None,
		};

		let resolved_table = ResolvedTable::new(alter.sequence.name.clone(), namespace, table_def);

		let resolved_source = ResolvedSource::Table(resolved_table);

		let column_def = ColumnDef {
			id: ColumnId(1),
			name: alter.column.name.text().to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8), // Placeholder
			policies: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
		};

		let resolved_column = ResolvedColumn::new(alter.column.name.clone(), resolved_source, column_def);

		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: resolved_sequence,
			column: resolved_column,
			value: alter.value,
		}))
	}
}
