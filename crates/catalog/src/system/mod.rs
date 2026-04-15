// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	catalog::vtable::{VTable, VTableId},
	version::SystemVersion,
};

pub mod authentications;
pub mod cdc_consumers;
pub mod column_properties;
pub mod columns;
pub mod configs;
pub mod dictionaries;
pub mod enum_variants;
pub mod enums;
pub mod event_variants;
pub mod events;
pub mod flow_edges;
pub mod flow_lags;
pub mod flow_node_types;
pub mod flow_nodes;
pub mod flow_operator_inputs;
pub mod flow_operator_outputs;
pub mod flow_operators;
pub mod flows;
pub mod granted_roles;
pub mod handlers;
pub mod identities;
pub mod metrics_cdc;
pub mod metrics_storage;
pub mod migrations;
pub mod namespaces;
pub mod operator_retention_strategies;
pub mod policies;
pub mod policy_operations;
pub mod primary_key_columns;
pub mod primary_keys;
pub mod procedures;
pub mod ringbuffers;
pub mod roles;
pub mod sequence;
pub mod series;
pub mod shape_fields;
pub mod shape_retention_strategies;
pub mod shapes;
pub mod subscriptions;
pub mod tables;
pub mod tables_virtual;
pub mod tag_variants;
pub mod tags;
pub mod types;
pub mod versions;
pub mod views;
pub mod virtual_table_columns;

use authentications::authentications;
use cdc_consumers::cdc_consumers;
use column_properties::column_properties;
use columns::columns;
use configs::configs;
use dictionaries::dictionaries;
use enum_variants::enum_variants;
use enums::enums;
use event_variants::event_variants;
use events::events;
use flow_edges::flow_edges;
use flow_lags::flow_lags;
use flow_node_types::flow_node_types;
use flow_nodes::flow_nodes;
use flow_operator_inputs::flow_operator_inputs;
use flow_operator_outputs::flow_operator_outputs;
use flow_operators::flow_operators;
use flows::flows;
use granted_roles::granted_roles;
use handlers::handlers;
use identities::identities;
use metrics_cdc::metrics_cdc_vtable;
use metrics_storage::metrics_storage_vtable;
use migrations::migrations;
use namespaces::namespaces;
use operator_retention_strategies::operator_retention_strategies;
use policies::policies;
use policy_operations::policy_operations;
use primary_key_columns::primary_key_columns;
use primary_keys::primary_keys;
use procedures::procedures;
use roles::roles;
use sequence::sequences;
use series::series;
use shape_fields::shape_fields;
use shape_retention_strategies::shape_retention_strategies;
use shapes::shapes;
use subscriptions::subscriptions;
use tables::tables;
use tables_virtual::virtual_tables;
use tag_variants::tag_variants;
use tags::tags;
use types::types;
use versions::versions;
use views::views;
use virtual_table_columns::virtual_table_columns;

use crate::system::ringbuffers::ringbuffers;

/// Nine slots, one per primitive variant shared between storage and cdc
/// (table, view, table_virtual, ringbuffer, dictionary, series, flow,
/// flow_node, system).
const METRIC_PRIMITIVE_SLOTS: usize = 9;

static METRICS_STORAGE_CACHE: [OnceLock<Arc<VTable>>; METRIC_PRIMITIVE_SLOTS] = [
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
];

static METRICS_CDC_CACHE: [OnceLock<Arc<VTable>>; METRIC_PRIMITIVE_SLOTS] = [
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
	OnceLock::new(),
];

fn metrics_storage_table_cached(id: VTableId, local_name: &str, slot: usize) -> Arc<VTable> {
	METRICS_STORAGE_CACHE[slot].get_or_init(|| metrics_storage_vtable(id, local_name)).clone()
}

fn metrics_cdc_table_cached(id: VTableId, local_name: &str, slot: usize) -> Arc<VTable> {
	METRICS_CDC_CACHE[slot].get_or_init(|| metrics_cdc_vtable(id, local_name)).clone()
}

pub mod ids {
	pub mod columns {
		pub mod cdc_consumers {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const CONSUMER_ID: ColumnId = ColumnId(1);
			pub const CHECKPOINT: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [CONSUMER_ID, CHECKPOINT];
		}

		pub mod sequences {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, VALUE];
		}

		pub mod namespaces {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const PARENT_ID: ColumnId = ColumnId(3);
			pub const LOCAL_NAME: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAME, LOCAL_NAME, PARENT_ID];
		}

		pub mod tables {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, PRIMARY_KEY_ID];
		}

		pub mod views {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, KIND, PRIMARY_KEY_ID];
		}

		pub mod flows {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const STATUS: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, STATUS];
		}

		pub mod flow_nodes {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const FLOW_ID: ColumnId = ColumnId(2);
			pub const NODE_TYPE: ColumnId = ColumnId(3);
			pub const DATA: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, FLOW_ID, NODE_TYPE, DATA];
		}

		pub mod flow_edges {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const FLOW_ID: ColumnId = ColumnId(2);
			pub const SOURCE: ColumnId = ColumnId(3);
			pub const TARGET: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, FLOW_ID, SOURCE, TARGET];
		}

		#[allow(clippy::module_inception)]
		pub mod columns {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SHAPE_ID: ColumnId = ColumnId(2);
			pub const SHAPE_TYPE: ColumnId = ColumnId(3);
			pub const NAME: ColumnId = ColumnId(4);
			pub const TYPE: ColumnId = ColumnId(5);
			pub const POSITION: ColumnId = ColumnId(6);
			pub const AUTO_INCREMENT: ColumnId = ColumnId(7);
			pub const DICTIONARY_ID: ColumnId = ColumnId(8);

			pub const ALL: [ColumnId; 8] =
				[ID, SHAPE_ID, SHAPE_TYPE, NAME, TYPE, POSITION, AUTO_INCREMENT, DICTIONARY_ID];
		}

		pub mod enum_variants {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const VARIANT_TAG: ColumnId = ColumnId(2);
			pub const VARIANT_NAME: ColumnId = ColumnId(3);
			pub const FIELD_COUNT: ColumnId = ColumnId(4);
			pub const FIELD_INDEX: ColumnId = ColumnId(5);
			pub const FIELD_NAME: ColumnId = ColumnId(6);
			pub const FIELD_TYPE: ColumnId = ColumnId(7);

			pub const ALL: [ColumnId; 7] =
				[ID, VARIANT_TAG, VARIANT_NAME, FIELD_COUNT, FIELD_INDEX, FIELD_NAME, FIELD_TYPE];
		}

		pub mod enums {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, NAMESPACE_ID, NAME];
		}

		pub mod event_variants {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const VARIANT_TAG: ColumnId = ColumnId(2);
			pub const VARIANT_NAME: ColumnId = ColumnId(3);
			pub const FIELD_COUNT: ColumnId = ColumnId(4);
			pub const FIELD_INDEX: ColumnId = ColumnId(5);
			pub const FIELD_NAME: ColumnId = ColumnId(6);
			pub const FIELD_TYPE: ColumnId = ColumnId(7);

			pub const ALL: [ColumnId; 7] =
				[ID, VARIANT_TAG, VARIANT_NAME, FIELD_COUNT, FIELD_INDEX, FIELD_NAME, FIELD_TYPE];
		}

		pub mod events {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, NAMESPACE_ID, NAME];
		}

		pub mod procedures {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const IS_TEST: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, IS_TEST];
		}

		pub mod tag_variants {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const VARIANT_TAG: ColumnId = ColumnId(2);
			pub const VARIANT_NAME: ColumnId = ColumnId(3);
			pub const FIELD_COUNT: ColumnId = ColumnId(4);
			pub const FIELD_INDEX: ColumnId = ColumnId(5);
			pub const FIELD_NAME: ColumnId = ColumnId(6);
			pub const FIELD_TYPE: ColumnId = ColumnId(7);

			pub const ALL: [ColumnId; 7] =
				[ID, VARIANT_TAG, VARIANT_NAME, FIELD_COUNT, FIELD_INDEX, FIELD_NAME, FIELD_TYPE];
		}

		pub mod tags {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, NAMESPACE_ID, NAME];
		}

		pub mod series {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TAG_ID: ColumnId = ColumnId(4);
			pub const KEY_COLUMN: ColumnId = ColumnId(5);
			pub const KEY_KIND: ColumnId = ColumnId(6);

			pub const ALL: [ColumnId; 6] = [ID, NAMESPACE_ID, NAME, TAG_ID, KEY_COLUMN, KEY_KIND];
		}

		pub mod handlers {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const ON_SUMTYPE_ID: ColumnId = ColumnId(4);
			pub const ON_VARIANT_TAG: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, ON_SUMTYPE_ID, ON_VARIANT_TAG];
		}

		pub mod migrations {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const NAME: ColumnId = ColumnId(1);
			pub const ACTION: ColumnId = ColumnId(2);
			pub const BODY: ColumnId = ColumnId(3);
			pub const ROLLBACK_BODY: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [NAME, ACTION, BODY, ROLLBACK_BODY];
		}

		pub mod dictionaries {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const VALUE_TYPE: ColumnId = ColumnId(4);
			pub const ID_TYPE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, VALUE_TYPE, ID_TYPE];
		}

		pub mod primary_keys {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const SHAPE_ID: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, SHAPE_ID];
		}

		pub mod ringbuffers {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const CAPACITY: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, CAPACITY, PRIMARY_KEY_ID];
		}

		pub mod primary_key_columns {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const POSITION: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [PRIMARY_KEY_ID, COLUMN_ID, POSITION];
		}

		pub mod column_properties {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const COLUMN_ID: ColumnId = ColumnId(2);
			pub const TYPE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, COLUMN_ID, TYPE, VALUE];
		}

		pub mod versions {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const NAME: ColumnId = ColumnId(1);
			pub const VERSION: ColumnId = ColumnId(2);
			pub const DESCRIPTION: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [NAME, VERSION, DESCRIPTION, TYPE];
		}

		pub mod configs {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const KEY: ColumnId = ColumnId(1);
			pub const VALUE: ColumnId = ColumnId(2);
			pub const DEFAULT_VALUE: ColumnId = ColumnId(3);
			pub const DESCRIPTION: ColumnId = ColumnId(4);
			pub const REQUIRES_RESTART: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [KEY, VALUE, DEFAULT_VALUE, DESCRIPTION, REQUIRES_RESTART];
		}

		pub mod shape_retention_strategies {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const SHAPE_ID: ColumnId = ColumnId(1);
			pub const SHAPE_TYPE: ColumnId = ColumnId(2);
			pub const STRATEGY_TYPE: ColumnId = ColumnId(3);
			pub const CLEANUP_MODE: ColumnId = ColumnId(4);
			pub const VALUE: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [SHAPE_ID, SHAPE_TYPE, STRATEGY_TYPE, CLEANUP_MODE, VALUE];
		}

		pub mod operator_retention_strategies {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const OPERATOR_ID: ColumnId = ColumnId(1);
			pub const STRATEGY_TYPE: ColumnId = ColumnId(2);
			pub const CLEANUP_MODE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [OPERATOR_ID, STRATEGY_TYPE, CLEANUP_MODE, VALUE];
		}

		pub mod flow_operators {
			use reifydb_core::interface::catalog::id::ColumnId;

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
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const POSITION: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const DESCRIPTION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [OPERATOR, POSITION, NAME, TYPE, DESCRIPTION];
		}

		pub mod flow_operator_outputs {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const POSITION: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const DESCRIPTION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [OPERATOR, POSITION, NAME, TYPE, DESCRIPTION];
		}

		pub mod virtual_tables {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, KIND];
		}

		pub mod flow_lags {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const FLOW_ID: ColumnId = ColumnId(1);
			pub const SHAPE_ID: ColumnId = ColumnId(2);
			pub const LAG: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [FLOW_ID, SHAPE_ID, LAG];
		}

		pub mod subscriptions {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const COLUMN_COUNT: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, COLUMN_COUNT];
		}

		pub mod shapes {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const FINGERPRINT: ColumnId = ColumnId(1);
			pub const FIELD_COUNT: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [FINGERPRINT, FIELD_COUNT];
		}

		pub mod shape_fields {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const SHAPE_FINGERPRINT: ColumnId = ColumnId(1);
			pub const FIELD_INDEX: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const CONSTRAINT_TYPE: ColumnId = ColumnId(5);
			pub const CONSTRAINT_P1: ColumnId = ColumnId(6);
			pub const CONSTRAINT_P2: ColumnId = ColumnId(7);
			pub const OFFSET: ColumnId = ColumnId(8);
			pub const SIZE: ColumnId = ColumnId(9);
			pub const ALIGN: ColumnId = ColumnId(10);

			pub const ALL: [ColumnId; 10] = [
				SHAPE_FINGERPRINT,
				FIELD_INDEX,
				NAME,
				TYPE,
				CONSTRAINT_TYPE,
				CONSTRAINT_P1,
				CONSTRAINT_P2,
				OFFSET,
				SIZE,
				ALIGN,
			];
		}

		pub mod users {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const ENABLED: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, NAME, ENABLED];
		}

		pub mod roles {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, NAME];
		}

		pub mod granted_roles {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const IDENTITY_ID: ColumnId = ColumnId(1);
			pub const ROLE_ID: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [IDENTITY_ID, ROLE_ID];
		}

		pub mod policies {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const TARGET_TYPE: ColumnId = ColumnId(3);
			pub const TARGET_NAMESPACE: ColumnId = ColumnId(4);
			pub const TARGET_SHAPE: ColumnId = ColumnId(5);
			pub const ENABLED: ColumnId = ColumnId(6);

			pub const ALL: [ColumnId; 6] = [ID, NAME, TARGET_TYPE, TARGET_NAMESPACE, TARGET_SHAPE, ENABLED];
		}

		pub mod authentications {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const USER_ID: ColumnId = ColumnId(2);
			pub const METHOD: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, USER_ID, METHOD];
		}

		pub mod policy_operations {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const POLICY_ID: ColumnId = ColumnId(1);
			pub const OPERATION: ColumnId = ColumnId(2);
			pub const BODY_SOURCE: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [POLICY_ID, OPERATION, BODY_SOURCE];
		}

		pub mod virtual_table_columns {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const VTABLE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const POSITION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [ID, VTABLE_ID, NAME, TYPE, POSITION];
		}
	}

	pub mod sequences {
		use reifydb_core::interface::catalog::id::SequenceId;

		pub const NAMESPACE: SequenceId = SequenceId(1);
		pub const SOURCE: SequenceId = SequenceId(2);
		pub const COLUMN: SequenceId = SequenceId(3);
		pub const COLUMN_PROPERTY: SequenceId = SequenceId(4);
		pub const FLOW: SequenceId = SequenceId(5);
		pub const FLOW_NODE: SequenceId = SequenceId(6);
		pub const FLOW_EDGE: SequenceId = SequenceId(7);
		pub const PRIMARY_KEY: SequenceId = SequenceId(8);
		pub const PROCEDURE: SequenceId = SequenceId(9);
		pub const HANDLER: SequenceId = SequenceId(10);
		pub const USER: SequenceId = SequenceId(11);
		pub const ROLE: SequenceId = SequenceId(12);
		pub const POLICY: SequenceId = SequenceId(13);
		pub const MIGRATION: SequenceId = SequenceId(14);
		pub const MIGRATION_EVENT: SequenceId = SequenceId(15);
		pub const AUTHENTICATION: SequenceId = SequenceId(16);
		pub const TEST: SequenceId = SequenceId(17);
		pub const TOKEN: SequenceId = SequenceId(18);
		pub const SOURCE_CONNECTOR: SequenceId = SequenceId(19);
		pub const SINK_CONNECTOR: SequenceId = SequenceId(20);

		pub const ALL: [SequenceId; 20] = [
			NAMESPACE,
			SOURCE,
			COLUMN,
			COLUMN_PROPERTY,
			FLOW,
			FLOW_NODE,
			FLOW_EDGE,
			PRIMARY_KEY,
			PROCEDURE,
			HANDLER,
			USER,
			ROLE,
			POLICY,
			MIGRATION,
			MIGRATION_EVENT,
			AUTHENTICATION,
			TEST,
			TOKEN,
			SOURCE_CONNECTOR,
			SINK_CONNECTOR,
		];
	}

	pub mod vtable {
		use reifydb_core::interface::catalog::vtable::VTableId;

		pub const SEQUENCES: VTableId = VTableId(1);
		pub const NAMESPACES: VTableId = VTableId(2);
		pub const TABLES: VTableId = VTableId(3);
		pub const VIEWS: VTableId = VTableId(4);
		pub const FLOWS: VTableId = VTableId(13);
		pub const COLUMNS: VTableId = VTableId(5);
		pub const COLUMN_PROPERTIES: VTableId = VTableId(6);
		pub const PRIMARY_KEYS: VTableId = VTableId(7);
		pub const PRIMARY_KEY_COLUMNS: VTableId = VTableId(8);
		pub const VERSIONS: VTableId = VTableId(9);
		pub const PRIMITIVE_RETENTION_STRATEGIES: VTableId = VTableId(10);
		pub const OPERATOR_RETENTION_STRATEGIES: VTableId = VTableId(11);
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
		pub const FLOW_LAGS: VTableId = VTableId(31);
		pub const SHAPES: VTableId = VTableId(32);
		pub const SHAPE_FIELDS: VTableId = VTableId(33);
		pub const ENUMS: VTableId = VTableId(34);
		pub const EVENTS: VTableId = VTableId(35);
		pub const PROCEDURES: VTableId = VTableId(36);
		pub const HANDLERS: VTableId = VTableId(37);
		pub const TAGS: VTableId = VTableId(38);
		pub const SERIES: VTableId = VTableId(39);
		pub const IDENTITIES: VTableId = VTableId(40);
		pub const ROLES: VTableId = VTableId(41);
		pub const GRANTED_ROLES: VTableId = VTableId(42);
		pub const POLICIES: VTableId = VTableId(43);
		pub const POLICY_OPERATIONS: VTableId = VTableId(44);
		pub const MIGRATIONS: VTableId = VTableId(45);
		pub const AUTHENTICATIONS: VTableId = VTableId(46);
		pub const CONFIGS: VTableId = VTableId(47);
		pub const VIRTUAL_TABLE_COLUMNS: VTableId = VTableId(48);
		pub const ENUM_VARIANTS: VTableId = VTableId(49);
		pub const EVENT_VARIANTS: VTableId = VTableId(50);
		pub const TAG_VARIANTS: VTableId = VTableId(51);
		pub const SUBSCRIPTIONS: VTableId = VTableId(52);

		// `system::metrics::storage::*` virtual tables.
		pub const METRICS_STORAGE_TABLE: VTableId = VTableId(1024);
		pub const METRICS_STORAGE_VIEW: VTableId = VTableId(1025);
		pub const METRICS_STORAGE_TABLE_VIRTUAL: VTableId = VTableId(1026);
		pub const METRICS_STORAGE_RINGBUFFER: VTableId = VTableId(1027);
		pub const METRICS_STORAGE_DICTIONARY: VTableId = VTableId(1028);
		pub const METRICS_STORAGE_SERIES: VTableId = VTableId(1029);
		pub const METRICS_STORAGE_FLOW: VTableId = VTableId(1030);
		pub const METRICS_STORAGE_FLOW_NODE: VTableId = VTableId(1031);
		pub const METRICS_STORAGE_SYSTEM: VTableId = VTableId(1032);

		// `system::metrics::cdc::*` virtual tables.
		pub const METRICS_CDC_TABLE: VTableId = VTableId(1033);
		pub const METRICS_CDC_VIEW: VTableId = VTableId(1034);
		pub const METRICS_CDC_TABLE_VIRTUAL: VTableId = VTableId(1035);
		pub const METRICS_CDC_RINGBUFFER: VTableId = VTableId(1036);
		pub const METRICS_CDC_DICTIONARY: VTableId = VTableId(1037);
		pub const METRICS_CDC_SERIES: VTableId = VTableId(1038);
		pub const METRICS_CDC_FLOW: VTableId = VTableId(1039);
		pub const METRICS_CDC_FLOW_NODE: VTableId = VTableId(1040);
		pub const METRICS_CDC_SYSTEM: VTableId = VTableId(1041);

		pub const ALL: [VTableId; 63] = [
			SEQUENCES,
			NAMESPACES,
			TABLES,
			VIEWS,
			FLOWS,
			COLUMNS,
			COLUMN_PROPERTIES,
			PRIMARY_KEYS,
			PRIMARY_KEY_COLUMNS,
			VERSIONS,
			PRIMITIVE_RETENTION_STRATEGIES,
			OPERATOR_RETENTION_STRATEGIES,
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
			FLOW_LAGS,
			SHAPES,
			SHAPE_FIELDS,
			ENUMS,
			EVENTS,
			PROCEDURES,
			HANDLERS,
			TAGS,
			SERIES,
			IDENTITIES,
			ROLES,
			GRANTED_ROLES,
			POLICIES,
			POLICY_OPERATIONS,
			MIGRATIONS,
			AUTHENTICATIONS,
			CONFIGS,
			VIRTUAL_TABLE_COLUMNS,
			ENUM_VARIANTS,
			EVENT_VARIANTS,
			TAG_VARIANTS,
			SUBSCRIPTIONS,
			METRICS_STORAGE_TABLE,
			METRICS_STORAGE_VIEW,
			METRICS_STORAGE_TABLE_VIRTUAL,
			METRICS_STORAGE_RINGBUFFER,
			METRICS_STORAGE_DICTIONARY,
			METRICS_STORAGE_SERIES,
			METRICS_STORAGE_FLOW,
			METRICS_STORAGE_FLOW_NODE,
			METRICS_STORAGE_SYSTEM,
			METRICS_CDC_TABLE,
			METRICS_CDC_VIEW,
			METRICS_CDC_TABLE_VIRTUAL,
			METRICS_CDC_RINGBUFFER,
			METRICS_CDC_DICTIONARY,
			METRICS_CDC_SERIES,
			METRICS_CDC_FLOW,
			METRICS_CDC_FLOW_NODE,
			METRICS_CDC_SYSTEM,
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
	pub fn get_system_sequences_table() -> Arc<VTable> {
		sequences()
	}

	/// Get the namespaces virtual table definition
	pub fn get_system_namespaces_table() -> Arc<VTable> {
		namespaces()
	}

	/// Get the tables virtual table definition
	pub fn get_system_tables_table() -> Arc<VTable> {
		tables()
	}

	/// Get the views virtual table definition
	pub fn get_system_views_table() -> Arc<VTable> {
		views()
	}

	/// Get the flows virtual table definition
	pub fn get_system_flows_table() -> Arc<VTable> {
		flows()
	}

	/// Get the flow_lags virtual table definition
	pub fn get_system_flow_lags_table() -> Arc<VTable> {
		flow_lags()
	}

	/// Get the subscriptions virtual table definition
	pub fn get_system_subscriptions_table() -> Arc<VTable> {
		subscriptions()
	}

	/// Get the columns virtual table definition
	pub fn get_system_columns_table() -> Arc<VTable> {
		columns()
	}

	/// Get the primary_keys virtual table definition
	pub fn get_system_primary_keys_table() -> Arc<VTable> {
		primary_keys()
	}

	/// Get the primary_key_columns virtual table definition
	pub fn get_system_primary_key_columns_table() -> Arc<VTable> {
		primary_key_columns()
	}

	/// Get the column_properties virtual table definition
	pub fn get_system_column_properties_table() -> Arc<VTable> {
		column_properties()
	}

	/// Get the system versions virtual table definition
	pub fn get_system_versions_table() -> Arc<VTable> {
		versions()
	}

	/// Get the shape_retention_strategies virtual table definition
	pub fn get_system_shape_retention_strategies_table() -> Arc<VTable> {
		shape_retention_strategies()
	}

	/// Get the operator_retention_strategies virtual table definition
	pub fn get_system_operator_retention_strategies_table() -> Arc<VTable> {
		operator_retention_strategies()
	}

	/// Get the cdc_consumers virtual table definition
	pub fn get_system_cdc_consumers_table() -> Arc<VTable> {
		cdc_consumers()
	}

	/// Get the flow_operators virtual table definition
	pub fn get_system_flow_operators_table() -> Arc<VTable> {
		flow_operators()
	}

	/// Get the flow_nodes virtual table definition
	pub fn get_system_flow_nodes_table() -> Arc<VTable> {
		flow_nodes()
	}

	/// Get the flow_edges virtual table definition
	pub fn get_system_flow_edges_table() -> Arc<VTable> {
		flow_edges()
	}

	/// Get the dictionaries virtual table definition
	pub fn get_system_dictionaries_table() -> Arc<VTable> {
		dictionaries()
	}

	/// Get the virtual_tables virtual table definition
	pub fn get_system_virtual_tables_table() -> Arc<VTable> {
		virtual_tables()
	}

	/// Get the types virtual table definition
	pub fn get_system_types_table() -> Arc<VTable> {
		types()
	}

	/// Get the flow_node_types virtual table definition
	pub fn get_system_flow_node_types_table() -> Arc<VTable> {
		flow_node_types()
	}

	/// Get the flow_operator_inputs virtual table definition
	pub fn get_system_flow_operator_inputs_table() -> Arc<VTable> {
		flow_operator_inputs()
	}

	/// Get the flow_operator_outputs virtual table definition
	pub fn get_system_flow_operator_outputs_table() -> Arc<VTable> {
		flow_operator_outputs()
	}

	/// Get the ringbuffers virtual table definition
	pub fn get_system_ringbuffers_table() -> Arc<VTable> {
		ringbuffers()
	}

	pub fn get_system_metrics_storage_table_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_TABLE, "table", 0)
	}

	pub fn get_system_metrics_storage_view_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_VIEW, "view", 1)
	}

	pub fn get_system_metrics_storage_table_virtual_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_TABLE_VIRTUAL, "table_virtual", 2)
	}

	pub fn get_system_metrics_storage_ringbuffer_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_RINGBUFFER, "ringbuffer", 3)
	}

	pub fn get_system_metrics_storage_dictionary_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_DICTIONARY, "dictionary", 4)
	}

	pub fn get_system_metrics_storage_series_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_SERIES, "series", 5)
	}

	pub fn get_system_metrics_storage_flow_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_FLOW, "flow", 6)
	}

	pub fn get_system_metrics_storage_flow_node_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_FLOW_NODE, "flow_node", 7)
	}

	pub fn get_system_metrics_storage_system_table() -> Arc<VTable> {
		metrics_storage_table_cached(ids::vtable::METRICS_STORAGE_SYSTEM, "system", 8)
	}

	pub fn get_system_metrics_cdc_table_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_TABLE, "table", 0)
	}

	pub fn get_system_metrics_cdc_view_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_VIEW, "view", 1)
	}

	pub fn get_system_metrics_cdc_table_virtual_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_TABLE_VIRTUAL, "table_virtual", 2)
	}

	pub fn get_system_metrics_cdc_ringbuffer_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_RINGBUFFER, "ringbuffer", 3)
	}

	pub fn get_system_metrics_cdc_dictionary_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_DICTIONARY, "dictionary", 4)
	}

	pub fn get_system_metrics_cdc_series_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_SERIES, "series", 5)
	}

	pub fn get_system_metrics_cdc_flow_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_FLOW, "flow", 6)
	}

	pub fn get_system_metrics_cdc_flow_node_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_FLOW_NODE, "flow_node", 7)
	}

	pub fn get_system_metrics_cdc_system_table() -> Arc<VTable> {
		metrics_cdc_table_cached(ids::vtable::METRICS_CDC_SYSTEM, "system", 8)
	}

	/// Get the shapes virtual table definition
	pub fn get_system_shapes_table() -> Arc<VTable> {
		shapes()
	}

	/// Get the shape_fields virtual table definition
	pub fn get_system_shape_fields_table() -> Arc<VTable> {
		shape_fields()
	}

	/// Get the enums virtual table definition
	pub fn get_system_enums_table() -> Arc<VTable> {
		enums()
	}

	/// Get the enum_variants virtual table definition
	pub fn get_system_enum_variants_table() -> Arc<VTable> {
		enum_variants()
	}

	/// Get the events virtual table definition
	pub fn get_system_events_table() -> Arc<VTable> {
		events()
	}

	/// Get the event_variants virtual table definition
	pub fn get_system_event_variants_table() -> Arc<VTable> {
		event_variants()
	}

	/// Get the procedures virtual table definition
	pub fn get_system_procedures_table() -> Arc<VTable> {
		procedures()
	}

	/// Get the handlers virtual table definition
	pub fn get_system_handlers_table() -> Arc<VTable> {
		handlers()
	}

	/// Get the tags virtual table definition
	pub fn get_system_tags_table() -> Arc<VTable> {
		tags()
	}

	/// Get the tag_variants virtual table definition
	pub fn get_system_tag_variants_table() -> Arc<VTable> {
		tag_variants()
	}

	/// Get the series virtual table definition
	pub fn get_system_series_table() -> Arc<VTable> {
		series()
	}

	/// Get the identities virtual table definition
	pub fn get_system_identities_table() -> Arc<VTable> {
		identities()
	}

	/// Get the roles virtual table definition
	pub fn get_system_roles_table() -> Arc<VTable> {
		roles()
	}

	/// Get the granted_roles virtual table definition
	pub fn get_system_granted_roles_table() -> Arc<VTable> {
		granted_roles()
	}

	/// Get the policies virtual table definition
	pub fn get_system_policies_table() -> Arc<VTable> {
		policies()
	}

	/// Get the policy_operations virtual table definition
	pub fn get_system_policy_operations_table() -> Arc<VTable> {
		policy_operations()
	}

	/// Get the migrations virtual table definition
	pub fn get_system_migrations_table() -> Arc<VTable> {
		migrations()
	}

	/// Get the authentications virtual table definition
	pub fn get_system_authentications_table() -> Arc<VTable> {
		authentications()
	}

	/// Get the configs virtual table definition
	pub fn get_configs_table() -> Arc<VTable> {
		configs()
	}

	/// Get the virtual_table_columns virtual table definition
	pub fn get_system_virtual_table_columns_table() -> Arc<VTable> {
		virtual_table_columns()
	}
}
