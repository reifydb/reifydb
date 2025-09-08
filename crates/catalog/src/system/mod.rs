// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::TableVirtualDef;

mod column_policies;
mod columns;
mod primary_key_columns;
mod primary_keys;
mod schemas;
mod sequence;
mod tables;
mod views;

use column_policies::column_policies;
use columns::columns;
use primary_key_columns::primary_key_columns;
use primary_keys::primary_keys;
use schemas::schemas;
use sequence::sequences;
use tables::tables;
use views::views;

pub mod ids {
	pub mod columns {
		pub mod sequences {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SCHEMA_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 4] =
				[ID, SCHEMA_ID, NAME, VALUE];
		}

		pub mod schemas {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, NAME];
		}

		pub mod tables {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SCHEMA_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] =
				[ID, SCHEMA_ID, NAME, PRIMARY_KEY_ID];
		}

		pub mod views {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SCHEMA_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] =
				[ID, SCHEMA_ID, NAME, KIND, PRIMARY_KEY_ID];
		}

		pub mod columns {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SOURCE_ID: ColumnId = ColumnId(2);
			pub const SOURCE_TYPE: ColumnId = ColumnId(3);
			pub const NAME: ColumnId = ColumnId(4);
			pub const TYPE: ColumnId = ColumnId(5);
			pub const POSITION: ColumnId = ColumnId(6);
			pub const AUTO_INCREMENT: ColumnId = ColumnId(7);

			pub const ALL: [ColumnId; 7] = [
				ID,
				SOURCE_ID,
				SOURCE_TYPE,
				NAME,
				TYPE,
				POSITION,
				AUTO_INCREMENT,
			];
		}

		pub mod primary_keys {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SOURCE_ID: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, SOURCE_ID];
		}

		pub mod primary_key_columns {
			use reifydb_core::interface::ColumnId;

			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const POSITION: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] =
				[PRIMARY_KEY_ID, COLUMN_ID, POSITION];
		}

		pub mod column_policies {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const TYPE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] =
				[ID, COLUMN_ID, TYPE, VALUE];
		}
	}

	pub mod sequences {
		use reifydb_core::interface::SequenceId;

		pub const SCHEMA: SequenceId = SequenceId(1);
		pub const SOURCE: SequenceId = SequenceId(2);
		pub const COLUMN: SequenceId = SequenceId(3);
		pub const COLUMN_POLICY: SequenceId = SequenceId(4);
		pub const FLOW: SequenceId = SequenceId(5);
		pub const FLOW_NODE: SequenceId = SequenceId(6);
		pub const FLOW_EDGE: SequenceId = SequenceId(7);
		pub const PRIMARY_KEY: SequenceId = SequenceId(8);

		pub const ALL: [SequenceId; 8] = [
			SCHEMA,
			SOURCE,
			COLUMN,
			COLUMN_POLICY,
			FLOW,
			FLOW_NODE,
			FLOW_EDGE,
			PRIMARY_KEY,
		];
	}

	pub mod table_virtual {
		use reifydb_core::interface::TableVirtualId;

		pub const SEQUENCES: TableVirtualId = TableVirtualId(1);
		pub const SCHEMAS: TableVirtualId = TableVirtualId(2);
		pub const TABLES: TableVirtualId = TableVirtualId(3);
		pub const VIEWS: TableVirtualId = TableVirtualId(4);
		pub const COLUMNS: TableVirtualId = TableVirtualId(5);
		pub const COLUMN_POLICIES: TableVirtualId = TableVirtualId(6);
		pub const PRIMARY_KEYS: TableVirtualId = TableVirtualId(7);
		pub const PRIMARY_KEY_COLUMNS: TableVirtualId =
			TableVirtualId(8);

		pub const ALL: [TableVirtualId; 8] = [
			SEQUENCES,
			SCHEMAS,
			TABLES,
			VIEWS,
			COLUMNS,
			COLUMN_POLICIES,
			PRIMARY_KEYS,
			PRIMARY_KEY_COLUMNS,
		];
	}
}

pub struct SystemCatalog;

impl SystemCatalog {
	/// Get the sequences virtual table definition
	pub fn sequences() -> Arc<TableVirtualDef> {
		sequences()
	}

	/// Get the schemas virtual table definition
	pub fn schemas() -> Arc<TableVirtualDef> {
		schemas()
	}

	/// Get the tables virtual table definition
	pub fn tables() -> Arc<TableVirtualDef> {
		tables()
	}

	/// Get the views virtual table definition
	pub fn views() -> Arc<TableVirtualDef> {
		views()
	}

	/// Get the columns virtual table definition
	pub fn columns() -> Arc<TableVirtualDef> {
		columns()
	}

	/// Get the primary_keys virtual table definition
	pub fn primary_keys() -> Arc<TableVirtualDef> {
		primary_keys()
	}

	/// Get the primary_key_columns virtual table definition
	pub fn primary_key_columns() -> Arc<TableVirtualDef> {
		primary_key_columns()
	}

	/// Get the column_policies virtual table definition
	pub fn column_policies() -> Arc<TableVirtualDef> {
		column_policies()
	}
}
