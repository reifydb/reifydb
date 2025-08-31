// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::TableVirtualDef;
use sequence::sequences;

mod sequence;

pub mod ids {
	pub mod columns {
		pub mod sequences {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SCHEMA_ID: ColumnId = ColumnId(2);
			pub const SCHEMA_NAME: ColumnId = ColumnId(3);
			pub const NAME: ColumnId = ColumnId(4);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const COL_ALL: [ColumnId; 5] =
				[ID, SCHEMA_ID, SCHEMA_NAME, NAME, VALUE];
		}
	}

	pub mod sequences {
		use reifydb_core::interface::SequenceId;

		pub const SCHEMA: SequenceId = SequenceId(1);
		pub const STORE: SequenceId = SequenceId(2);
		pub const COLUMN: SequenceId = SequenceId(3);
		pub const COLUMN_POLICY: SequenceId = SequenceId(4);
		pub const FLOW: SequenceId = SequenceId(5);
		pub const FLOW_NODE: SequenceId = SequenceId(6);
		pub const FLOW_EDGE: SequenceId = SequenceId(7);

		pub const ALL: [SequenceId; 7] = [
			SCHEMA,
			STORE,
			COLUMN,
			COLUMN_POLICY,
			FLOW,
			FLOW_NODE,
			FLOW_EDGE,
		];
	}

	pub mod table_virtual {
		use reifydb_core::interface::TableVirtualId;

		pub const SEQUENCES: TableVirtualId = TableVirtualId(1);
	}
}

pub struct SystemCatalog;

impl SystemCatalog {
	/// Get the sequences virtual table definition
	pub fn sequences() -> Arc<TableVirtualDef> {
		sequences()
	}
}
