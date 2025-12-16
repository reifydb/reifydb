// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{TableVirtualDef, version::SystemVersion};

mod cdc_consumers;
mod column_policies;
mod columns;
mod dictionaries;
mod flow_edges;
mod flow_node_types;
mod flow_nodes;
mod flow_operator_inputs;
mod flow_operator_outputs;
mod flow_operators;
mod flows;
mod namespaces;
mod operator_retention_policies;
mod primary_key_columns;
mod primary_keys;
mod ringbuffers;
mod sequence;
mod source_retention_policies;
mod tables;
mod tables_virtual;
mod types;
mod versions;
mod views;

use cdc_consumers::cdc_consumers;
use column_policies::column_policies;
use columns::columns;
use dictionaries::dictionaries;
use flow_edges::flow_edges;
use flow_node_types::flow_node_types;
use flow_nodes::flow_nodes;
use flow_operator_inputs::flow_operator_inputs;
use flow_operator_outputs::flow_operator_outputs;
use flow_operators::flow_operators;
use flows::flows;
use namespaces::namespaces;
use operator_retention_policies::operator_retention_policies;
use primary_key_columns::primary_key_columns;
use primary_keys::primary_keys;
use sequence::sequences;
use source_retention_policies::source_retention_policies;
use tables::tables;
use tables_virtual::virtual_tables;
use types::types;
use versions::versions;
use views::views;

use crate::system::ringbuffers::ringbuffers;

pub mod ids {
	pub mod columns {
		pub mod cdc_consumers {
			use reifydb_core::interface::ColumnId;

			pub const CONSUMER_ID: ColumnId = ColumnId(1);
			pub const CHECKPOINT: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [CONSUMER_ID, CHECKPOINT];
		}

		pub mod sequences {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, VALUE];
		}

		pub mod namespaces {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, NAME];
		}

		pub mod tables {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, PRIMARY_KEY_ID];
		}

		pub mod views {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, KIND, PRIMARY_KEY_ID];
		}

		pub mod flows {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const STATUS: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, STATUS];
		}

		pub mod flow_nodes {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const FLOW_ID: ColumnId = ColumnId(2);
			pub const NODE_TYPE: ColumnId = ColumnId(3);
			pub const DATA: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, FLOW_ID, NODE_TYPE, DATA];
		}

		pub mod flow_edges {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const FLOW_ID: ColumnId = ColumnId(2);
			pub const SOURCE: ColumnId = ColumnId(3);
			pub const TARGET: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, FLOW_ID, SOURCE, TARGET];
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
			pub const DICTIONARY_ID: ColumnId = ColumnId(8);

			pub const ALL: [ColumnId; 8] =
				[ID, SOURCE_ID, SOURCE_TYPE, NAME, TYPE, POSITION, AUTO_INCREMENT, DICTIONARY_ID];
		}

		pub mod dictionaries {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const VALUE_TYPE: ColumnId = ColumnId(4);
			pub const ID_TYPE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, VALUE_TYPE, ID_TYPE];
		}

		pub mod primary_keys {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SOURCE_ID: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, SOURCE_ID];
		}

		pub mod ringbuffers {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const CAPACITY: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, CAPACITY, PRIMARY_KEY_ID];
		}

		pub mod primary_key_columns {
			use reifydb_core::interface::ColumnId;

			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const POSITION: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [PRIMARY_KEY_ID, COLUMN_ID, POSITION];
		}

		pub mod column_policies {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const TYPE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, COLUMN_ID, TYPE, VALUE];
		}

		pub mod versions {
			use reifydb_core::interface::ColumnId;

			pub const NAME: ColumnId = ColumnId(1);
			pub const VERSION: ColumnId = ColumnId(2);
			pub const DESCRIPTION: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [NAME, VERSION, DESCRIPTION, TYPE];
		}

		pub mod source_retention_policies {
			use reifydb_core::interface::ColumnId;

			pub const SOURCE_ID: ColumnId = ColumnId(1);
			pub const SOURCE_TYPE: ColumnId = ColumnId(2);
			pub const POLICY_TYPE: ColumnId = ColumnId(3);
			pub const CLEANUP_MODE: ColumnId = ColumnId(4);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [SOURCE_ID, SOURCE_TYPE, POLICY_TYPE, CLEANUP_MODE, VALUE];
		}

		pub mod operator_retention_policies {
			use reifydb_core::interface::ColumnId;

			pub const OPERATOR_ID: ColumnId = ColumnId(1);
			pub const POLICY_TYPE: ColumnId = ColumnId(2);
			pub const CLEANUP_MODE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [OPERATOR_ID, POLICY_TYPE, CLEANUP_MODE, VALUE];
		}

		pub mod flow_operators {
			use reifydb_core::interface::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const LIBRARY_PATH: ColumnId = ColumnId(2);
			pub const API: ColumnId = ColumnId(3);
			pub const CAP_INSERT: ColumnId = ColumnId(4);
			pub const CAP_UPDATE: ColumnId = ColumnId(5);
			pub const CAP_DELETE: ColumnId = ColumnId(6);
			pub const CAP_DROP: ColumnId = ColumnId(7);
			pub const CAP_GET_ROWS: ColumnId = ColumnId(8);
			pub const CAP_TICK: ColumnId = ColumnId(9);

			pub const ALL: [ColumnId; 9] = [
				OPERATOR,
				LIBRARY_PATH,
				API,
				CAP_INSERT,
				CAP_UPDATE,
				CAP_DELETE,
				CAP_GET_ROWS,
				CAP_DROP,
				CAP_TICK,
			];
		}

		pub mod flow_operator_inputs {
			use reifydb_core::interface::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const POSITION: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const DESCRIPTION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [OPERATOR, POSITION, NAME, TYPE, DESCRIPTION];
		}

		pub mod flow_operator_outputs {
			use reifydb_core::interface::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const POSITION: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const DESCRIPTION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [OPERATOR, POSITION, NAME, TYPE, DESCRIPTION];
		}

		pub mod virtual_tables {
			use reifydb_core::interface::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, KIND];
		}
	}

	pub mod sequences {
		use reifydb_core::interface::SequenceId;

		pub const NAMESPACE: SequenceId = SequenceId(1);
		pub const SOURCE: SequenceId = SequenceId(2);
		pub const COLUMN: SequenceId = SequenceId(3);
		pub const COLUMN_POLICY: SequenceId = SequenceId(4);
		pub const FLOW: SequenceId = SequenceId(5);
		pub const FLOW_NODE: SequenceId = SequenceId(6);
		pub const FLOW_EDGE: SequenceId = SequenceId(7);
		pub const PRIMARY_KEY: SequenceId = SequenceId(8);

		pub const ALL: [SequenceId; 8] =
			[NAMESPACE, SOURCE, COLUMN, COLUMN_POLICY, FLOW, FLOW_NODE, FLOW_EDGE, PRIMARY_KEY];
	}

	pub mod table_virtual {
		use reifydb_core::interface::TableVirtualId;

		pub const SEQUENCES: TableVirtualId = TableVirtualId(1);
		pub const NAMESPACES: TableVirtualId = TableVirtualId(2);
		pub const TABLES: TableVirtualId = TableVirtualId(3);
		pub const VIEWS: TableVirtualId = TableVirtualId(4);
		pub const FLOWS: TableVirtualId = TableVirtualId(13);
		pub const COLUMNS: TableVirtualId = TableVirtualId(5);
		pub const COLUMN_POLICIES: TableVirtualId = TableVirtualId(6);
		pub const PRIMARY_KEYS: TableVirtualId = TableVirtualId(7);
		pub const PRIMARY_KEY_COLUMNS: TableVirtualId = TableVirtualId(8);
		pub const VERSIONS: TableVirtualId = TableVirtualId(9);
		pub const SOURCE_RETENTION_POLICIES: TableVirtualId = TableVirtualId(10);
		pub const OPERATOR_RETENTION_POLICIES: TableVirtualId = TableVirtualId(11);
		pub const CDC_CONSUMERS: TableVirtualId = TableVirtualId(12);
		pub const FLOW_OPERATORS: TableVirtualId = TableVirtualId(14);
		pub const FLOW_NODES: TableVirtualId = TableVirtualId(15);
		pub const FLOW_EDGES: TableVirtualId = TableVirtualId(16);
		pub const DICTIONARIES: TableVirtualId = TableVirtualId(17);
		pub const VIRTUAL_TABLES: TableVirtualId = TableVirtualId(18);
		pub const TYPES: TableVirtualId = TableVirtualId(19);
		pub const FLOW_NODE_TYPES: TableVirtualId = TableVirtualId(20);
		pub const FLOW_OPERATOR_INPUTS: TableVirtualId = TableVirtualId(21);
		pub const FLOW_OPERATOR_OUTPUTS: TableVirtualId = TableVirtualId(22);
		pub const RINGBUFFERS: TableVirtualId = TableVirtualId(23);

		pub const ALL: [TableVirtualId; 23] = [
			SEQUENCES,
			NAMESPACES,
			TABLES,
			VIEWS,
			FLOWS,
			COLUMNS,
			COLUMN_POLICIES,
			PRIMARY_KEYS,
			PRIMARY_KEY_COLUMNS,
			VERSIONS,
			SOURCE_RETENTION_POLICIES,
			OPERATOR_RETENTION_POLICIES,
			CDC_CONSUMERS,
			FLOW_OPERATORS,
			FLOW_NODES,
			FLOW_EDGES,
			DICTIONARIES,
			VIRTUAL_TABLES,
			TYPES,
			FLOW_NODE_TYPES,
			FLOW_OPERATOR_INPUTS,
			FLOW_OPERATOR_OUTPUTS,
			RINGBUFFERS,
		];
	}
}

#[derive(Clone, Debug)]
pub struct SystemCatalog(Arc<SystemCatalogInner>);

#[derive(Debug)]
struct SystemCatalogInner {
	versions: Vec<SystemVersion>,
}

impl SystemCatalog {
	/// Create a new SystemCatalog with the provided
	/// versions are set once at construction and never change
	pub fn new(versions: Vec<SystemVersion>) -> Self {
		Self(Arc::new(SystemCatalogInner {
			versions,
		}))
	}

	/// Get all system versions
	pub fn get_system_versions(&self) -> &[SystemVersion] {
		&self.0.versions
	}

	/// Get the sequences virtual table definition
	pub fn get_system_sequences_table_def() -> Arc<TableVirtualDef> {
		sequences()
	}

	/// Get the namespaces virtual table definition
	pub fn get_system_namespaces_table_def() -> Arc<TableVirtualDef> {
		namespaces()
	}

	/// Get the tables virtual table definition
	pub fn get_system_tables_table_def() -> Arc<TableVirtualDef> {
		tables()
	}

	/// Get the views virtual table definition
	pub fn get_system_views_table_def() -> Arc<TableVirtualDef> {
		views()
	}

	/// Get the flows virtual table definition
	pub fn get_system_flows_table_def() -> Arc<TableVirtualDef> {
		flows()
	}

	/// Get the columns virtual table definition
	pub fn get_system_columns_table_def() -> Arc<TableVirtualDef> {
		columns()
	}

	/// Get the primary_keys virtual table definition
	pub fn get_system_primary_keys_table_def() -> Arc<TableVirtualDef> {
		primary_keys()
	}

	/// Get the primary_key_columns virtual table definition
	pub fn get_system_primary_key_columns_table_def() -> Arc<TableVirtualDef> {
		primary_key_columns()
	}

	/// Get the column_policies virtual table definition
	pub fn get_system_column_policies_table_def() -> Arc<TableVirtualDef> {
		column_policies()
	}

	/// Get the system versions virtual table definition
	pub fn get_system_versions_table_def() -> Arc<TableVirtualDef> {
		versions()
	}

	/// Get the source_retention_policies virtual table definition
	pub fn get_system_source_retention_policies_table_def() -> Arc<TableVirtualDef> {
		source_retention_policies()
	}

	/// Get the operator_retention_policies virtual table definition
	pub fn get_system_operator_retention_policies_table_def() -> Arc<TableVirtualDef> {
		operator_retention_policies()
	}

	/// Get the cdc_consumers virtual table definition
	pub fn get_system_cdc_consumers_table_def() -> Arc<TableVirtualDef> {
		cdc_consumers()
	}

	/// Get the flow_operators virtual table definition
	pub fn get_system_flow_operators_table_def() -> Arc<TableVirtualDef> {
		flow_operators()
	}

	/// Get the flow_nodes virtual table definition
	pub fn get_system_flow_nodes_table_def() -> Arc<TableVirtualDef> {
		flow_nodes()
	}

	/// Get the flow_edges virtual table definition
	pub fn get_system_flow_edges_table_def() -> Arc<TableVirtualDef> {
		flow_edges()
	}

	/// Get the dictionaries virtual table definition
	pub fn get_system_dictionaries_table_def() -> Arc<TableVirtualDef> {
		dictionaries()
	}

	/// Get the virtual_tables virtual table definition
	pub fn get_system_virtual_tables_table_def() -> Arc<TableVirtualDef> {
		virtual_tables()
	}

	/// Get the types virtual table definition
	pub fn get_system_types_table_def() -> Arc<TableVirtualDef> {
		types()
	}

	/// Get the flow_node_types virtual table definition
	pub fn get_system_flow_node_types_table_def() -> Arc<TableVirtualDef> {
		flow_node_types()
	}

	/// Get the flow_operator_inputs virtual table definition
	pub fn get_system_flow_operator_inputs_table_def() -> Arc<TableVirtualDef> {
		flow_operator_inputs()
	}

	/// Get the flow_operator_outputs virtual table definition
	pub fn get_system_flow_operator_outputs_table_def() -> Arc<TableVirtualDef> {
		flow_operator_outputs()
	}

	/// Get the ringbuffers virtual table definition
	pub fn get_system_ringbuffers_table_def() -> Arc<TableVirtualDef> {
		ringbuffers()
	}
}
