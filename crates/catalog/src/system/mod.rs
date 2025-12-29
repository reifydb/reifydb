// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{VTableDef, version::SystemVersion};

mod cdc_consumers;
mod column_policies;
mod columns;
mod dictionaries;
mod flow_edges;
mod flow_lags;
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
mod primitive_retention_policies;
mod ringbuffers;
mod sequence;
mod storage_stats_dictionary;
mod storage_stats_flow;
mod storage_stats_flow_node;
mod storage_stats_index;
mod storage_stats_ringbuffer;
mod storage_stats_table;
mod storage_stats_view;
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
use flow_lags::flow_lags;
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
use primitive_retention_policies::primitive_retention_policies;
use sequence::sequences;
use storage_stats_dictionary::dictionary_storage_stats;
use storage_stats_flow::flow_storage_stats;
use storage_stats_flow_node::flow_node_storage_stats;
use storage_stats_index::index_storage_stats;
use storage_stats_ringbuffer::ringbuffer_storage_stats;
use storage_stats_table::table_storage_stats;
use storage_stats_view::view_storage_stats;
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

		pub mod primitive_retention_policies {
			use reifydb_core::interface::ColumnId;

			pub const PRIMITIVE_ID: ColumnId = ColumnId(1);
			pub const PRIMITIVE_TYPE: ColumnId = ColumnId(2);
			pub const POLICY_TYPE: ColumnId = ColumnId(3);
			pub const CLEANUP_MODE: ColumnId = ColumnId(4);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [PRIMITIVE_ID, PRIMITIVE_TYPE, POLICY_TYPE, CLEANUP_MODE, VALUE];
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
			pub const CAP_PULL: ColumnId = ColumnId(8);
			pub const CAP_TICK: ColumnId = ColumnId(9);

			pub const ALL: [ColumnId; 9] = [
				OPERATOR,
				LIBRARY_PATH,
				API,
				CAP_INSERT,
				CAP_UPDATE,
				CAP_DELETE,
				CAP_PULL,
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

		pub mod flow_lags {
			use reifydb_core::interface::ColumnId;

			pub const FLOW_ID: ColumnId = ColumnId(1);
			pub const PRIMITIVE_ID: ColumnId = ColumnId(2);
			pub const LAG: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [FLOW_ID, PRIMITIVE_ID, LAG];
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

	pub mod vtable {
		use reifydb_core::interface::VTableId;

		pub const SEQUENCES: VTableId = VTableId(1);
		pub const NAMESPACES: VTableId = VTableId(2);
		pub const TABLES: VTableId = VTableId(3);
		pub const VIEWS: VTableId = VTableId(4);
		pub const FLOWS: VTableId = VTableId(13);
		pub const COLUMNS: VTableId = VTableId(5);
		pub const COLUMN_POLICIES: VTableId = VTableId(6);
		pub const PRIMARY_KEYS: VTableId = VTableId(7);
		pub const PRIMARY_KEY_COLUMNS: VTableId = VTableId(8);
		pub const VERSIONS: VTableId = VTableId(9);
		pub const PRIMITIVE_RETENTION_POLICIES: VTableId = VTableId(10);
		pub const OPERATOR_RETENTION_POLICIES: VTableId = VTableId(11);
		pub const CDC_CONSUMERS: VTableId = VTableId(12);
		pub const FLOW_OPERATORS: VTableId = VTableId(14);
		pub const FLOW_NODES: VTableId = VTableId(15);
		pub const FLOW_EDGES: VTableId = VTableId(16);
		pub const DICTIONARIES: VTableId = VTableId(17);
		pub const VIRTUAL_TABLES: VTableId = VTableId(18);
		pub const TYPES: VTableId = VTableId(19);
		pub const FLOW_NODE_TYPES: VTableId = VTableId(20);
		pub const FLOW_OPERATOR_INPUTS: VTableId = VTableId(21);
		pub const FLOW_OPERATOR_OUTPUTS: VTableId = VTableId(22);
		pub const RINGBUFFERS: VTableId = VTableId(23);
		pub const TABLE_STORAGE_STATS: VTableId = VTableId(24);
		pub const VIEW_STORAGE_STATS: VTableId = VTableId(25);
		pub const FLOW_STORAGE_STATS: VTableId = VTableId(26);
		pub const FLOW_NODE_STORAGE_STATS: VTableId = VTableId(27);
		pub const INDEX_STORAGE_STATS: VTableId = VTableId(28);
		pub const RINGBUFFER_STORAGE_STATS: VTableId = VTableId(29);
		pub const DICTIONARY_STORAGE_STATS: VTableId = VTableId(30);
		pub const FLOW_LAGS: VTableId = VTableId(31);

		pub const ALL: [VTableId; 31] = [
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
			PRIMITIVE_RETENTION_POLICIES,
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
			TABLE_STORAGE_STATS,
			VIEW_STORAGE_STATS,
			FLOW_STORAGE_STATS,
			FLOW_NODE_STORAGE_STATS,
			INDEX_STORAGE_STATS,
			RINGBUFFER_STORAGE_STATS,
			DICTIONARY_STORAGE_STATS,
			FLOW_LAGS,
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
	pub fn get_system_sequences_table_def() -> Arc<VTableDef> {
		sequences()
	}

	/// Get the namespaces virtual table definition
	pub fn get_system_namespaces_table_def() -> Arc<VTableDef> {
		namespaces()
	}

	/// Get the tables virtual table definition
	pub fn get_system_tables_table_def() -> Arc<VTableDef> {
		tables()
	}

	/// Get the views virtual table definition
	pub fn get_system_views_table_def() -> Arc<VTableDef> {
		views()
	}

	/// Get the flows virtual table definition
	pub fn get_system_flows_table_def() -> Arc<VTableDef> {
		flows()
	}

	/// Get the flow_lags virtual table definition
	pub fn get_system_flow_lags_table_def() -> Arc<VTableDef> {
		flow_lags()
	}

	/// Get the columns virtual table definition
	pub fn get_system_columns_table_def() -> Arc<VTableDef> {
		columns()
	}

	/// Get the primary_keys virtual table definition
	pub fn get_system_primary_keys_table_def() -> Arc<VTableDef> {
		primary_keys()
	}

	/// Get the primary_key_columns virtual table definition
	pub fn get_system_primary_key_columns_table_def() -> Arc<VTableDef> {
		primary_key_columns()
	}

	/// Get the column_policies virtual table definition
	pub fn get_system_column_policies_table_def() -> Arc<VTableDef> {
		column_policies()
	}

	/// Get the system versions virtual table definition
	pub fn get_system_versions_table_def() -> Arc<VTableDef> {
		versions()
	}

	/// Get the primitive_retention_policies virtual table definition
	pub fn get_system_primitive_retention_policies_table_def() -> Arc<VTableDef> {
		primitive_retention_policies()
	}

	/// Get the operator_retention_policies virtual table definition
	pub fn get_system_operator_retention_policies_table_def() -> Arc<VTableDef> {
		operator_retention_policies()
	}

	/// Get the cdc_consumers virtual table definition
	pub fn get_system_cdc_consumers_table_def() -> Arc<VTableDef> {
		cdc_consumers()
	}

	/// Get the flow_operators virtual table definition
	pub fn get_system_flow_operators_table_def() -> Arc<VTableDef> {
		flow_operators()
	}

	/// Get the flow_nodes virtual table definition
	pub fn get_system_flow_nodes_table_def() -> Arc<VTableDef> {
		flow_nodes()
	}

	/// Get the flow_edges virtual table definition
	pub fn get_system_flow_edges_table_def() -> Arc<VTableDef> {
		flow_edges()
	}

	/// Get the dictionaries virtual table definition
	pub fn get_system_dictionaries_table_def() -> Arc<VTableDef> {
		dictionaries()
	}

	/// Get the virtual_tables virtual table definition
	pub fn get_system_virtual_tables_table_def() -> Arc<VTableDef> {
		virtual_tables()
	}

	/// Get the types virtual table definition
	pub fn get_system_types_table_def() -> Arc<VTableDef> {
		types()
	}

	/// Get the flow_node_types virtual table definition
	pub fn get_system_flow_node_types_table_def() -> Arc<VTableDef> {
		flow_node_types()
	}

	/// Get the flow_operator_inputs virtual table definition
	pub fn get_system_flow_operator_inputs_table_def() -> Arc<VTableDef> {
		flow_operator_inputs()
	}

	/// Get the flow_operator_outputs virtual table definition
	pub fn get_system_flow_operator_outputs_table_def() -> Arc<VTableDef> {
		flow_operator_outputs()
	}

	/// Get the ringbuffers virtual table definition
	pub fn get_system_ringbuffers_table_def() -> Arc<VTableDef> {
		ringbuffers()
	}

	/// Get the table_storage_stats virtual table definition
	pub fn get_system_table_storage_stats_table_def() -> Arc<VTableDef> {
		table_storage_stats()
	}

	/// Get the view_storage_stats virtual table definition
	pub fn get_system_view_storage_stats_table_def() -> Arc<VTableDef> {
		view_storage_stats()
	}

	/// Get the flow_storage_stats virtual table definition
	pub fn get_system_flow_storage_stats_table_def() -> Arc<VTableDef> {
		flow_storage_stats()
	}

	/// Get the flow_node_storage_stats virtual table definition
	pub fn get_system_flow_node_storage_stats_table_def() -> Arc<VTableDef> {
		flow_node_storage_stats()
	}

	/// Get the index_storage_stats virtual table definition
	pub fn get_system_index_storage_stats_table_def() -> Arc<VTableDef> {
		index_storage_stats()
	}

	/// Get the ringbuffer_storage_stats virtual table definition
	pub fn get_system_ringbuffer_storage_stats_table_def() -> Arc<VTableDef> {
		ringbuffer_storage_stats()
	}

	/// Get the dictionary_storage_stats virtual table definition
	pub fn get_system_dictionary_storage_stats_table_def() -> Arc<VTableDef> {
		dictionary_storage_stats()
	}
}
