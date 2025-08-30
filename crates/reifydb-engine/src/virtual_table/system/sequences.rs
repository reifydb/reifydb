// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, interface::virtual_table::VirtualTableDef,
	value::columnar::Columns,
};

use crate::virtual_table::{VirtualTable, VirtualTableQueryContext};

/// Virtual table that exposes system sequence information
pub struct Sequences {
	definition: VirtualTableDef,
}

impl Sequences {
	pub fn new(definition: VirtualTableDef) -> Self {
		Self {
			definition,
		}
	}
}

impl VirtualTable for Sequences {
	fn query(&self, _ctx: VirtualTableQueryContext) -> Result<Columns> {
		use crate::columnar::{Column, ColumnData, ColumnQualified};

		// TODO: Read actual sequence data from SystemSequence
		// For now, return hardcoded example data
		let sequence_names = vec![
			"schema_sequence".to_string(),
			"table_sequence".to_string(),
			"column_sequence".to_string(),
		];

		let current_values = vec![1025i64, 1025i64, 100i64];

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "sequence_name".to_string(),
				data: ColumnData::utf8(sequence_names),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "current_value".to_string(),
				data: ColumnData::int8(current_values),
			}),
		];

		Ok(Columns::new(columns))
	}

	fn definition(&self) -> &VirtualTableDef {
		&self.definition
	}
}
