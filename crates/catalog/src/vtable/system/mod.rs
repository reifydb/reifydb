// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
pub mod metrics;
pub mod migrations;
pub mod namespaces;
pub mod node_retention_store;
pub mod operator_libraries;
pub mod operator_library_inputs;
pub mod operator_library_outputs;
pub mod operator_store;
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
pub mod sequences;
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

#[cfg(test)]
mod schema_conformance_tests;
