// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Self-hosted system tables: every catalog object kind ReifyDB stores about itself, declared here
//! as a virtual table so it can be queried with regular RQL.

use std::sync::Arc;

use reifydb_core::interface::{catalog::vtable::VTable, version::SystemVersion};

pub mod authentications;
pub mod bindings;
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
pub mod flow_watermarks;
pub mod flows;
pub mod granted_roles;
pub mod handlers;
pub mod identities;
pub mod identity_attribute_values;
pub mod identity_attributes;
pub mod migrations;
pub mod namespaces;
pub mod operator_libraries;
pub mod operator_library_inputs;
pub mod operator_library_outputs;
pub mod operator_types;
pub mod operators;
pub mod policies;
pub mod policy_operations;
pub mod primary_key_columns;
pub mod primary_keys;
pub mod procedures;
pub mod queues;
pub mod relationships;
pub mod ringbuffers;
pub mod roles;
pub mod row_shape_fields;
pub mod row_shapes;
pub mod sequence;
pub mod series;
pub mod subscription_watermarks;
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
use bindings::{grpc::bindings_grpc, http::bindings_http, ws::bindings_ws};
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
use flow_watermarks::flow_watermarks;
use flows::flows;
use granted_roles::granted_roles;
use handlers::handlers;
use identities::identities;
use identity_attribute_values::identity_attribute_values;
use identity_attributes::identity_attributes;
use migrations::migrations;
use namespaces::namespaces;
use operator_libraries::operator_libraries;
use operator_library_inputs::operator_library_inputs;
use operator_library_outputs::operator_library_outputs;
use operator_types::operator_types;
use operators::operators;
use policies::policies;
use policy_operations::policy_operations;
use primary_key_columns::primary_key_columns;
use primary_keys::primary_keys;
use procedures::{
	extern_c::procedures_extern_c, extern_wasm::procedures_extern_wasm, in_process::procedures_in_process,
	rql::procedures_rql, test::procedures_test,
};
use relationships::relationships;
use roles::roles;
use row_shape_fields::row_shape_fields;
use row_shapes::row_shapes;
use sequence::sequences;
use series::series;
use subscription_watermarks::subscription_watermarks;
use subscriptions::subscriptions;
use tables::tables;
use tables_virtual::virtual_tables;
use tag_variants::tag_variants;
use tags::tags;
use types::types;
use versions::versions;
use views::views;
use virtual_table_columns::virtual_table_columns;

use crate::system::{queues::queues, ringbuffers::ringbuffers};

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
			pub const TIME: ColumnId = ColumnId(5);
			pub const TS: ColumnId = ColumnId(6);

			pub const ALL: [ColumnId; 6] = [ID, NAMESPACE_ID, NAME, PRIMARY_KEY_ID, TIME, TS];
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

		pub mod operators {
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
			pub const OBJECT_ID: ColumnId = ColumnId(2);
			pub const OBJECT_TYPE: ColumnId = ColumnId(3);
			pub const NAME: ColumnId = ColumnId(4);
			pub const TYPE: ColumnId = ColumnId(5);
			pub const POSITION: ColumnId = ColumnId(6);
			pub const AUTO_INCREMENT: ColumnId = ColumnId(7);
			pub const DICTIONARY_ID: ColumnId = ColumnId(8);

			pub const ALL: [ColumnId; 8] =
				[ID, OBJECT_ID, OBJECT_TYPE, NAME, TYPE, POSITION, AUTO_INCREMENT, DICTIONARY_ID];
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
			pub mod rql {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const NAME: ColumnId = ColumnId(3);
				pub const RETURN_TYPE: ColumnId = ColumnId(4);
				pub const BODY: ColumnId = ColumnId(5);
				pub const TRIGGER_KIND: ColumnId = ColumnId(6);
				pub const EVENT_VARIANT_SUMTYPE_ID: ColumnId = ColumnId(7);
				pub const EVENT_VARIANT_INDEX: ColumnId = ColumnId(8);

				pub const ALL: [ColumnId; 8] = [
					ID,
					NAMESPACE_ID,
					NAME,
					RETURN_TYPE,
					BODY,
					TRIGGER_KIND,
					EVENT_VARIANT_SUMTYPE_ID,
					EVENT_VARIANT_INDEX,
				];
			}

			pub mod test {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const NAME: ColumnId = ColumnId(3);
				pub const RETURN_TYPE: ColumnId = ColumnId(4);
				pub const BODY: ColumnId = ColumnId(5);

				pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, RETURN_TYPE, BODY];
			}

			pub mod in_process {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const NAME: ColumnId = ColumnId(3);
				pub const HANDLER_NAME: ColumnId = ColumnId(4);

				pub const ALL: [ColumnId; 4] = [ID, NAMESPACE_ID, NAME, HANDLER_NAME];
			}

			pub mod extern_c {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const NAME: ColumnId = ColumnId(3);
				pub const HANDLER_NAME: ColumnId = ColumnId(4);
				pub const LIBRARY_PATH: ColumnId = ColumnId(5);
				pub const ENTRY_SYMBOL: ColumnId = ColumnId(6);

				pub const ALL: [ColumnId; 6] =
					[ID, NAMESPACE_ID, NAME, HANDLER_NAME, LIBRARY_PATH, ENTRY_SYMBOL];
			}

			pub mod extern_wasm {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const NAME: ColumnId = ColumnId(3);
				pub const HANDLER_NAME: ColumnId = ColumnId(4);
				pub const MODULE_ID: ColumnId = ColumnId(5);

				pub const ALL: [ColumnId; 5] = [ID, NAMESPACE_ID, NAME, HANDLER_NAME, MODULE_ID];
			}
		}

		pub mod bindings {
			pub mod http {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const PROCEDURE_ID: ColumnId = ColumnId(3);
				pub const NAME: ColumnId = ColumnId(4);
				pub const METHOD: ColumnId = ColumnId(5);
				pub const PATH: ColumnId = ColumnId(6);
				pub const FORMAT: ColumnId = ColumnId(7);

				pub const ALL: [ColumnId; 7] =
					[ID, NAMESPACE_ID, PROCEDURE_ID, NAME, METHOD, PATH, FORMAT];
			}

			pub mod grpc {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const PROCEDURE_ID: ColumnId = ColumnId(3);
				pub const NAME: ColumnId = ColumnId(4);
				pub const RPC_NAME: ColumnId = ColumnId(5);
				pub const FORMAT: ColumnId = ColumnId(6);

				pub const ALL: [ColumnId; 6] = [ID, NAMESPACE_ID, PROCEDURE_ID, NAME, RPC_NAME, FORMAT];
			}

			pub mod ws {
				use reifydb_core::interface::catalog::id::ColumnId;

				pub const ID: ColumnId = ColumnId(1);
				pub const NAMESPACE_ID: ColumnId = ColumnId(2);
				pub const PROCEDURE_ID: ColumnId = ColumnId(3);
				pub const NAME: ColumnId = ColumnId(4);
				pub const RPC_NAME: ColumnId = ColumnId(5);
				pub const FORMAT: ColumnId = ColumnId(6);

				pub const ALL: [ColumnId; 6] = [ID, NAMESPACE_ID, PROCEDURE_ID, NAME, RPC_NAME, FORMAT];
			}
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

			pub const TIME: ColumnId = ColumnId(7);
			pub const TS: ColumnId = ColumnId(8);

			pub const ALL: [ColumnId; 8] = [ID, NAMESPACE_ID, NAME, TAG_ID, KEY_COLUMN, KEY_KIND, TIME, TS];
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
			pub const OBJECT_ID: ColumnId = ColumnId(2);

			pub const ALL: [ColumnId; 2] = [ID, OBJECT_ID];
		}

		pub mod queues {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const PARTITIONS: ColumnId = ColumnId(4);
			pub const ORDERED_BY: ColumnId = ColumnId(5);
			pub const DEDUPLICATE_BY: ColumnId = ColumnId(6);
			pub const DEDUPLICATE_TTL: ColumnId = ColumnId(7);

			pub const TIME: ColumnId = ColumnId(8);
			pub const TS: ColumnId = ColumnId(9);

			pub const ALL: [ColumnId; 9] = [
				ID,
				NAMESPACE_ID,
				NAME,
				PARTITIONS,
				ORDERED_BY,
				DEDUPLICATE_BY,
				DEDUPLICATE_TTL,
				TIME,
				TS,
			];
		}

		pub mod relationships {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const SOURCE_TABLE_ID: ColumnId = ColumnId(4);
			pub const SOURCE_COLUMN_ID: ColumnId = ColumnId(5);
			pub const TARGET_TABLE_ID: ColumnId = ColumnId(6);
			pub const TARGET_COLUMN_ID: ColumnId = ColumnId(7);
			pub const JUNCTION_TABLE_ID: ColumnId = ColumnId(8);
			pub const JUNCTION_SOURCE_COLUMN_ID: ColumnId = ColumnId(9);
			pub const JUNCTION_TARGET_COLUMN_ID: ColumnId = ColumnId(10);
			pub const CARDINALITY: ColumnId = ColumnId(11);

			pub const ALL: [ColumnId; 11] = [
				ID,
				NAMESPACE_ID,
				NAME,
				SOURCE_TABLE_ID,
				SOURCE_COLUMN_ID,
				TARGET_TABLE_ID,
				TARGET_COLUMN_ID,
				JUNCTION_TABLE_ID,
				JUNCTION_SOURCE_COLUMN_ID,
				JUNCTION_TARGET_COLUMN_ID,
				CARDINALITY,
			];
		}

		pub mod ringbuffers {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAMESPACE_ID: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const CAPACITY: ColumnId = ColumnId(4);
			pub const PRIMARY_KEY_ID: ColumnId = ColumnId(5);

			pub const TIME: ColumnId = ColumnId(6);
			pub const TS: ColumnId = ColumnId(7);

			pub const ALL: [ColumnId; 7] = [ID, NAMESPACE_ID, NAME, CAPACITY, PRIMARY_KEY_ID, TIME, TS];
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

		pub mod operator_libraries {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const LIBRARY_PATH: ColumnId = ColumnId(2);
			pub const ABI: ColumnId = ColumnId(3);
			pub const CAP_INSERT: ColumnId = ColumnId(4);
			pub const CAP_UPDATE: ColumnId = ColumnId(5);
			pub const CAP_DELETE: ColumnId = ColumnId(6);

			pub const ALL: [ColumnId; 6] =
				[OPERATOR, LIBRARY_PATH, ABI, CAP_INSERT, CAP_UPDATE, CAP_DELETE];
		}

		pub mod operator_library_inputs {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const OPERATOR: ColumnId = ColumnId(1);
			pub const POSITION: ColumnId = ColumnId(2);
			pub const NAME: ColumnId = ColumnId(3);
			pub const TYPE: ColumnId = ColumnId(4);
			pub const DESCRIPTION: ColumnId = ColumnId(5);

			pub const ALL: [ColumnId; 5] = [OPERATOR, POSITION, NAME, TYPE, DESCRIPTION];
		}

		pub mod operator_library_outputs {
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

		pub mod flow_watermarks {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const FLOW_ID: ColumnId = ColumnId(1);
			pub const OBJECT_ID: ColumnId = ColumnId(2);
			pub const LAG: ColumnId = ColumnId(3);
			pub const OUTSTANDING: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [FLOW_ID, OBJECT_ID, LAG, OUTSTANDING];
		}

		pub mod subscription_watermarks {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const SUBSCRIPTION_ID: ColumnId = ColumnId(1);
			pub const OBJECT_ID: ColumnId = ColumnId(2);
			pub const LAG: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [SUBSCRIPTION_ID, OBJECT_ID, LAG];
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

			pub const ALL: [ColumnId; 9] = [
				SHAPE_FINGERPRINT,
				FIELD_INDEX,
				NAME,
				TYPE,
				CONSTRAINT_TYPE,
				CONSTRAINT_P1,
				CONSTRAINT_P2,
				OFFSET,
				SIZE,
			];
		}

		pub mod users {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const ENABLED: ColumnId = ColumnId(3);
			pub const KIND: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [ID, NAME, ENABLED, KIND];
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

		pub mod identity_attributes {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const VALUE_TYPE: ColumnId = ColumnId(3);

			pub const ALL: [ColumnId; 3] = [ID, NAME, VALUE_TYPE];
		}

		pub mod identity_attribute_values {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const IDENTITY: ColumnId = ColumnId(1);
			pub const ATTRIBUTE_ID: ColumnId = ColumnId(2);
			pub const ATTRIBUTE: ColumnId = ColumnId(3);
			pub const VALUE: ColumnId = ColumnId(4);

			pub const ALL: [ColumnId; 4] = [IDENTITY, ATTRIBUTE_ID, ATTRIBUTE, VALUE];
		}

		pub mod policies {
			use reifydb_core::interface::catalog::id::ColumnId;

			pub const ID: ColumnId = ColumnId(1);
			pub const NAME: ColumnId = ColumnId(2);
			pub const TARGET_TYPE: ColumnId = ColumnId(3);
			pub const TARGET_NAMESPACE: ColumnId = ColumnId(4);
			pub const TARGET_OBJECT: ColumnId = ColumnId(5);
			pub const ENABLED: ColumnId = ColumnId(6);

			pub const ALL: [ColumnId; 6] =
				[ID, NAME, TARGET_TYPE, TARGET_NAMESPACE, TARGET_OBJECT, ENABLED];
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
		pub const OPERATOR: SequenceId = SequenceId(6);
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
		pub const BINDING: SequenceId = SequenceId(21);
		pub const COLUMN_SNAPSHOT: SequenceId = SequenceId(22);
		pub const IDENTITY_ATTRIBUTE: SequenceId = SequenceId(23);
		pub const RELATIONSHIP: SequenceId = SequenceId(24);

		pub const ALL: [(SequenceId, &str); 24] = [
			(NAMESPACE, "namespace"),
			(SOURCE, "source"),
			(COLUMN, "column"),
			(COLUMN_PROPERTY, "column_property"),
			(FLOW, "flow"),
			(OPERATOR, "operator"),
			(FLOW_EDGE, "flow_edge"),
			(PRIMARY_KEY, "primary_key"),
			(PROCEDURE, "procedure"),
			(HANDLER, "handler"),
			(USER, "user"),
			(ROLE, "role"),
			(POLICY, "policy"),
			(MIGRATION, "migration"),
			(MIGRATION_EVENT, "migration_event"),
			(AUTHENTICATION, "authentication"),
			(TEST, "test"),
			(TOKEN, "token"),
			(SOURCE_CONNECTOR, "source_connector"),
			(SINK_CONNECTOR, "sink_connector"),
			(BINDING, "binding"),
			(COLUMN_SNAPSHOT, "column_snapshot"),
			(IDENTITY_ATTRIBUTE, "identity_attribute"),
			(RELATIONSHIP, "relationship"),
		];
	}

	pub mod vtable {
		use reifydb_core::interface::catalog::vtable::VTableId;

		pub const SEQUENCES: VTableId = VTableId(1);
		pub const NAMESPACES: VTableId = VTableId(2);
		pub const TABLES: VTableId = VTableId(3);
		pub const VIEWS: VTableId = VTableId(4);
		pub const FLOWS: VTableId = VTableId(11);
		pub const COLUMNS: VTableId = VTableId(5);
		pub const COLUMN_PROPERTIES: VTableId = VTableId(6);
		pub const PRIMARY_KEYS: VTableId = VTableId(7);
		pub const PRIMARY_KEY_COLUMNS: VTableId = VTableId(8);
		pub const VERSIONS: VTableId = VTableId(9);
		pub const CDC_CONSUMERS: VTableId = VTableId(10);
		pub const OPERATOR_LIBRARIES: VTableId = VTableId(12);
		pub const OPERATORS: VTableId = VTableId(13);
		pub const FLOW_EDGES: VTableId = VTableId(14);
		pub const DICTIONARIES: VTableId = VTableId(15);
		pub const VIRTUAL_TABLES: VTableId = VTableId(16);
		pub const TYPES: VTableId = VTableId(17);
		pub const OPERATOR_TYPES: VTableId = VTableId(18);
		pub const OPERATOR_LIBRARY_INPUTS: VTableId = VTableId(19);
		pub const OPERATOR_LIBRARY_OUTPUTS: VTableId = VTableId(20);
		pub const RINGBUFFERS: VTableId = VTableId(21);
		pub const FLOW_WATERMARKS: VTableId = VTableId(29);
		pub const SHAPES: VTableId = VTableId(30);
		pub const SHAPE_FIELDS: VTableId = VTableId(31);
		pub const ENUMS: VTableId = VTableId(32);
		pub const EVENTS: VTableId = VTableId(33);
		pub const HANDLERS: VTableId = VTableId(35);
		pub const TAGS: VTableId = VTableId(36);
		pub const SERIES: VTableId = VTableId(37);
		pub const IDENTITIES: VTableId = VTableId(38);
		pub const ROLES: VTableId = VTableId(39);
		pub const GRANTED_ROLES: VTableId = VTableId(40);
		pub const POLICIES: VTableId = VTableId(41);
		pub const POLICY_OPERATIONS: VTableId = VTableId(42);
		pub const MIGRATIONS: VTableId = VTableId(43);
		pub const AUTHENTICATIONS: VTableId = VTableId(44);
		pub const CONFIGS: VTableId = VTableId(45);
		pub const VIRTUAL_TABLE_COLUMNS: VTableId = VTableId(46);
		pub const ENUM_VARIANTS: VTableId = VTableId(47);
		pub const EVENT_VARIANTS: VTableId = VTableId(48);
		pub const TAG_VARIANTS: VTableId = VTableId(49);
		pub const SUBSCRIPTIONS: VTableId = VTableId(50);
		pub const SUBSCRIPTION_WATERMARKS: VTableId = VTableId(59);
		pub const IDENTITY_ATTRIBUTES: VTableId = VTableId(60);
		pub const IDENTITY_ATTRIBUTE_VALUES: VTableId = VTableId(61);
		pub const QUEUES: VTableId = VTableId(62);

		pub const PROCEDURES_RQL: VTableId = VTableId(51);
		pub const PROCEDURES_TEST: VTableId = VTableId(52);
		pub const PROCEDURES_IN_PROCESS: VTableId = VTableId(53);
		pub const PROCEDURES_EXTERN_C: VTableId = VTableId(54);
		pub const PROCEDURES_EXTERN_WASM: VTableId = VTableId(55);

		pub const BINDINGS_HTTP: VTableId = VTableId(56);
		pub const BINDINGS_GRPC: VTableId = VTableId(57);
		pub const BINDINGS_WS: VTableId = VTableId(58);

		pub const RELATIONSHIPS: VTableId = VTableId(64);

		pub const METRICS_STORAGE_TABLE: VTableId = VTableId(1024);
		pub const METRICS_STORAGE_VIEW: VTableId = VTableId(1025);
		pub const METRICS_STORAGE_TABLE_VIRTUAL: VTableId = VTableId(1026);
		pub const METRICS_STORAGE_RINGBUFFER: VTableId = VTableId(1027);
		pub const METRICS_STORAGE_DICTIONARY: VTableId = VTableId(1028);
		pub const METRICS_STORAGE_SERIES: VTableId = VTableId(1029);
		pub const METRICS_STORAGE_FLOW: VTableId = VTableId(1030);
		pub const METRICS_STORAGE_OPERATOR: VTableId = VTableId(1031);
		pub const METRICS_STORAGE_SYSTEM: VTableId = VTableId(1032);

		pub const METRICS_CDC_TABLE: VTableId = VTableId(1033);
		pub const METRICS_CDC_VIEW: VTableId = VTableId(1034);
		pub const METRICS_CDC_TABLE_VIRTUAL: VTableId = VTableId(1035);
		pub const METRICS_CDC_RINGBUFFER: VTableId = VTableId(1036);
		pub const METRICS_CDC_DICTIONARY: VTableId = VTableId(1037);
		pub const METRICS_CDC_SERIES: VTableId = VTableId(1038);
		pub const METRICS_CDC_FLOW: VTableId = VTableId(1039);
		pub const METRICS_CDC_OPERATOR: VTableId = VTableId(1040);
		pub const METRICS_CDC_SYSTEM: VTableId = VTableId(1041);

		pub const ALL: [VTableId; 73] = [
			SEQUENCES,
			SUBSCRIPTION_WATERMARKS,
			NAMESPACES,
			TABLES,
			VIEWS,
			FLOWS,
			COLUMNS,
			COLUMN_PROPERTIES,
			PRIMARY_KEYS,
			PRIMARY_KEY_COLUMNS,
			VERSIONS,
			CDC_CONSUMERS,
			OPERATOR_LIBRARIES,
			OPERATORS,
			FLOW_EDGES,
			DICTIONARIES,
			VIRTUAL_TABLES,
			TYPES,
			OPERATOR_TYPES,
			OPERATOR_LIBRARY_INPUTS,
			OPERATOR_LIBRARY_OUTPUTS,
			RINGBUFFERS,
			QUEUES,
			FLOW_WATERMARKS,
			SHAPES,
			SHAPE_FIELDS,
			ENUMS,
			EVENTS,
			PROCEDURES_RQL,
			PROCEDURES_TEST,
			PROCEDURES_IN_PROCESS,
			PROCEDURES_EXTERN_C,
			PROCEDURES_EXTERN_WASM,
			BINDINGS_HTTP,
			BINDINGS_GRPC,
			BINDINGS_WS,
			RELATIONSHIPS,
			HANDLERS,
			TAGS,
			SERIES,
			IDENTITIES,
			ROLES,
			GRANTED_ROLES,
			IDENTITY_ATTRIBUTES,
			IDENTITY_ATTRIBUTE_VALUES,
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
			METRICS_STORAGE_OPERATOR,
			METRICS_STORAGE_SYSTEM,
			METRICS_CDC_TABLE,
			METRICS_CDC_VIEW,
			METRICS_CDC_TABLE_VIRTUAL,
			METRICS_CDC_RINGBUFFER,
			METRICS_CDC_DICTIONARY,
			METRICS_CDC_SERIES,
			METRICS_CDC_FLOW,
			METRICS_CDC_OPERATOR,
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
	pub fn new(versions: Vec<SystemVersion>) -> Self {
		Self(Arc::new(SystemCatalogInner {
			versions,
		}))
	}

	pub fn get_system_versions(&self) -> &[SystemVersion] {
		&self.0.versions
	}

	pub fn get_system_sequences_table() -> Arc<VTable> {
		sequences()
	}

	pub fn get_system_namespaces_table() -> Arc<VTable> {
		namespaces()
	}

	pub fn get_system_tables_table() -> Arc<VTable> {
		tables()
	}

	pub fn get_system_views_table() -> Arc<VTable> {
		views()
	}

	pub fn get_system_flows_table() -> Arc<VTable> {
		flows()
	}

	pub fn get_system_flow_watermarks_table() -> Arc<VTable> {
		flow_watermarks()
	}

	pub fn get_system_subscription_watermarks_table() -> Arc<VTable> {
		subscription_watermarks()
	}

	pub fn get_system_subscriptions_table() -> Arc<VTable> {
		subscriptions()
	}

	pub fn get_system_columns_table() -> Arc<VTable> {
		columns()
	}

	pub fn get_system_primary_keys_table() -> Arc<VTable> {
		primary_keys()
	}

	pub fn get_system_relationships_table() -> Arc<VTable> {
		relationships()
	}

	pub fn get_system_primary_key_columns_table() -> Arc<VTable> {
		primary_key_columns()
	}

	pub fn get_system_column_properties_table() -> Arc<VTable> {
		column_properties()
	}

	pub fn get_system_versions_table() -> Arc<VTable> {
		versions()
	}

	pub fn get_system_cdc_consumers_table() -> Arc<VTable> {
		cdc_consumers()
	}

	pub fn get_system_operator_libraries_table() -> Arc<VTable> {
		operator_libraries()
	}

	pub fn get_system_operators_table() -> Arc<VTable> {
		operators()
	}

	pub fn get_system_flow_edges_table() -> Arc<VTable> {
		flow_edges()
	}

	pub fn get_system_dictionaries_table() -> Arc<VTable> {
		dictionaries()
	}

	pub fn get_system_virtual_tables_table() -> Arc<VTable> {
		virtual_tables()
	}

	pub fn get_system_types_table() -> Arc<VTable> {
		types()
	}

	pub fn get_system_operator_types_table() -> Arc<VTable> {
		operator_types()
	}

	pub fn get_system_operator_library_inputs_table() -> Arc<VTable> {
		operator_library_inputs()
	}

	pub fn get_system_operator_library_outputs_table() -> Arc<VTable> {
		operator_library_outputs()
	}

	pub fn get_system_ringbuffers_table() -> Arc<VTable> {
		ringbuffers()
	}

	pub fn get_system_queues_table() -> Arc<VTable> {
		queues()
	}

	pub fn get_system_row_shapes_table() -> Arc<VTable> {
		row_shapes()
	}

	pub fn get_system_row_shape_fields_table() -> Arc<VTable> {
		row_shape_fields()
	}

	pub fn get_system_enums_table() -> Arc<VTable> {
		enums()
	}

	pub fn get_system_enum_variants_table() -> Arc<VTable> {
		enum_variants()
	}

	pub fn get_system_events_table() -> Arc<VTable> {
		events()
	}

	pub fn get_system_event_variants_table() -> Arc<VTable> {
		event_variants()
	}

	pub fn get_system_procedures_rql_table() -> Arc<VTable> {
		procedures_rql()
	}

	pub fn get_system_procedures_test_table() -> Arc<VTable> {
		procedures_test()
	}

	pub fn get_system_procedures_in_process_table() -> Arc<VTable> {
		procedures_in_process()
	}

	pub fn get_system_procedures_extern_c_table() -> Arc<VTable> {
		procedures_extern_c()
	}

	pub fn get_system_procedures_extern_wasm_table() -> Arc<VTable> {
		procedures_extern_wasm()
	}

	pub fn get_system_bindings_http_table() -> Arc<VTable> {
		bindings_http()
	}

	pub fn get_system_bindings_grpc_table() -> Arc<VTable> {
		bindings_grpc()
	}

	pub fn get_system_bindings_ws_table() -> Arc<VTable> {
		bindings_ws()
	}

	pub fn get_system_handlers_table() -> Arc<VTable> {
		handlers()
	}

	pub fn get_system_tags_table() -> Arc<VTable> {
		tags()
	}

	pub fn get_system_tag_variants_table() -> Arc<VTable> {
		tag_variants()
	}

	pub fn get_system_series_table() -> Arc<VTable> {
		series()
	}

	pub fn get_system_identities_table() -> Arc<VTable> {
		identities()
	}

	pub fn get_system_roles_table() -> Arc<VTable> {
		roles()
	}

	pub fn get_system_granted_roles_table() -> Arc<VTable> {
		granted_roles()
	}

	pub fn get_system_identity_attributes_table() -> Arc<VTable> {
		identity_attributes()
	}

	pub fn get_system_identity_attribute_values_table() -> Arc<VTable> {
		identity_attribute_values()
	}

	pub fn get_system_policies_table() -> Arc<VTable> {
		policies()
	}

	pub fn get_system_policy_operations_table() -> Arc<VTable> {
		policy_operations()
	}

	pub fn get_system_migrations_table() -> Arc<VTable> {
		migrations()
	}

	pub fn get_system_authentications_table() -> Arc<VTable> {
		authentications()
	}

	pub fn get_configs_table() -> Arc<VTable> {
		configs()
	}

	pub fn get_system_virtual_table_columns_table() -> Arc<VTable> {
		virtual_table_columns()
	}
}
