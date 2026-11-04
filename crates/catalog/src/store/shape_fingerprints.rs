// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, path::Path};

use reifydb_codec::row::shape::RowShape;

use crate::store::{
	authentication::shape::authentication::SHAPE as AUTHENTICATION_SHAPE,
	authentication_method::shape::authentication_method::SHAPE as AUTHENTICATION_METHOD_SHAPE,
	binding::shape::{binding::SHAPE as BINDING_SHAPE, binding_namespace::SHAPE as BINDING_NAMESPACE_SHAPE},
	column::shape::{column::SHAPE as COLUMN_SHAPE, object_column::SHAPE as OBJECT_COLUMN_SHAPE},
	column_block::shape::column_block::SHAPE as COLUMN_BLOCK_SHAPE,
	column_property::shape::column_property::SHAPE as COLUMN_PROPERTY_SHAPE,
	config::shape::config::SHAPE as CONFIG_SHAPE,
	dictionary::shape::{
		dictionary::SHAPE as DICTIONARY_SHAPE, dictionary_namespace::SHAPE as DICTIONARY_NAMESPACE_SHAPE,
	},
	flow::shape::{flow::SHAPE as FLOW_SHAPE, flow_namespace::SHAPE as FLOW_NAMESPACE_SHAPE},
	flow_edge::shape::{flow_edge::SHAPE as FLOW_EDGE_SHAPE, flow_edge_by_flow::SHAPE as FLOW_EDGE_BY_FLOW_SHAPE},
	granted_role::shape::granted_role::SHAPE as GRANTED_ROLE_SHAPE,
	handler::shape::{handler::SHAPE as HANDLER_SHAPE, handler_namespace::SHAPE as HANDLER_NAMESPACE_SHAPE},
	identity::shape::identity::SHAPE as IDENTITY_SHAPE,
	identity_attribute::shape::identity_attribute::SHAPE as IDENTITY_ATTRIBUTE_SHAPE,
	identity_attribute_value::shape::identity_attribute_value::SHAPE as IDENTITY_ATTRIBUTE_VALUE_SHAPE,
	migration::shape::{migration::SHAPE as MIGRATION_SHAPE, migration_event::SHAPE as MIGRATION_EVENT_SHAPE},
	namespace::shape::namespace::SHAPE as NAMESPACE_SHAPE,
	operator::shape::{operator::SHAPE as OPERATOR_SHAPE, operator_by_flow::SHAPE as OPERATOR_BY_FLOW_SHAPE},
	operator_retention::shape::operator_retention::SHAPE as OPERATOR_RETENTION_SHAPE,
	policy::shape::{policy::SHAPE as POLICY_SHAPE, policy_op::SHAPE as POLICY_OP_SHAPE},
	primary_key::shape::primary_key::SHAPE as PRIMARY_KEY_SHAPE,
	procedure::shape::{
		namespace_procedure::SHAPE as NAMESPACE_PROCEDURE_SHAPE, procedure::SHAPE as PROCEDURE_SHAPE,
		procedure_param::SHAPE as PROCEDURE_PARAM_SHAPE,
	},
	queue::shape::{queue::SHAPE as QUEUE_SHAPE, queue_namespace::SHAPE as QUEUE_NAMESPACE_SHAPE},
	relationship::shape::relationship::SHAPE as RELATIONSHIP_SHAPE,
	ringbuffer::shape::{
		ringbuffer::SHAPE as RINGBUFFER_SHAPE, ringbuffer_namespace::SHAPE as RINGBUFFER_NAMESPACE_SHAPE,
	},
	role::shape::role::SHAPE as ROLE_SHAPE,
	row_settings::shape::row_settings::SHAPE as ROW_SETTINGS_SHAPE,
	row_shape::shape::shape_field::SHAPE as SHAPE_FIELD_SHAPE,
	series::shape::{series::SHAPE as SERIES_SHAPE, series_namespace::SHAPE as SERIES_NAMESPACE_SHAPE},
	sink::shape::{sink::SHAPE as SINK_SHAPE, sink_namespace::SHAPE as SINK_NAMESPACE_SHAPE},
	source::shape::{source::SHAPE as SOURCE_SHAPE, source_namespace::SHAPE as SOURCE_NAMESPACE_SHAPE},
	sumtype::shape::{sumtype::SHAPE as SUMTYPE_SHAPE, sumtype_namespace::SHAPE as SUMTYPE_NAMESPACE_SHAPE},
	table::shape::{table::SHAPE as TABLE_SHAPE, table_namespace::SHAPE as TABLE_NAMESPACE_SHAPE},
	token::shape::token::SHAPE as TOKEN_SHAPE,
	view::shape::{view::SHAPE as VIEW_SHAPE, view_namespace::SHAPE as VIEW_NAMESPACE_SHAPE},
	windowed_view_group::shape::{
		windowed_view_group::SHAPE as WINDOWED_VIEW_GROUP_SHAPE,
		windowed_view_group_namespace::SHAPE as WINDOWED_VIEW_GROUP_NAMESPACE_SHAPE,
	},
};

type Pin = (&'static str, &'static RowShape, u64, &'static [&'static str]);

fn shapes_declared_on_disk() -> usize {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/store");
	let mod_rs = fs::read_to_string(root.join("mod.rs")).unwrap();
	mod_rs.lines()
		.filter_map(|line| {
			let line = line.trim();
			line.strip_prefix("pub mod ").or_else(|| line.strip_prefix("pub(crate) mod "))
		})
		.filter_map(|line| line.strip_suffix(';'))
		.filter_map(|module| fs::read_to_string(root.join(module).join("shape.rs")).ok())
		.map(|source| source.matches("static SHAPE").count() + macro_authored_shapes(&source))
		.sum()
}

fn macro_authored_shapes(source: &str) -> usize {
	source.lines()
		.filter_map(|line| {
			let line = line.trim();
			line.strip_prefix("pub ").or_else(|| line.strip_prefix("pub(crate) "))
		})
		.filter(|rest| {
			let mut parts = rest.split_whitespace();
			let (Some(name), Some("{"), None) = (parts.next(), parts.next(), parts.next()) else {
				return false;
			};
			name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
		})
		.count()
}

fn pinned() -> Vec<Pin> {
	vec![
		(
			"authentication::authentication",
			&*AUTHENTICATION_SHAPE,
			0xa444757e7ac1ce67,
			&["id", "identity", "method", "properties"],
		),
		(
			"authentication_method::authentication_method",
			&*AUTHENTICATION_METHOD_SHAPE,
			0xa7d6456116c81106,
			&["id", "name", "ttl", "promote_guest", "properties"],
		),
		(
			"binding::binding",
			&*BINDING_SHAPE,
			0xcfeae7d7cd567f09,
			&[
				"id",
				"namespace",
				"name",
				"procedure_id",
				"protocol",
				"http_method",
				"http_path",
				"rpc_name",
				"format",
			],
		),
		("binding::binding_namespace", &*BINDING_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"column_property::column_property",
			&*COLUMN_PROPERTY_SHAPE,
			0xd5d8d32ddc4e7685,
			&["id", "column", "policy", "value"],
		),
		(
			"column::column",
			&*COLUMN_SHAPE,
			0x3cef33dd10d9f45c,
			&[
				"id",
				"object",
				"name",
				"value_type",
				"index",
				"auto_increment",
				"constraint",
				"dictionary_id",
			],
		),
		("column::object_column", &*OBJECT_COLUMN_SHAPE, 0xf634b05ae4ef37e2, &["id", "name", "index"]),
		(
			"column_block::column_block",
			&*COLUMN_BLOCK_SHAPE,
			0xbffe6445d29d3e7c,
			&[
				"id",
				"namespace",
				"kind",
				"source_id",
				"start",
				"end",
				"partition_hi",
				"partition_lo",
				"read_version",
				"row_count",
				"partition_values",
				"stats",
			],
		),
		("config::config", &*CONFIG_SHAPE, 0x100afb9034c112f2, &["value"]),
		(
			"dictionary::dictionary",
			&*DICTIONARY_SHAPE,
			0x20dfbda7eceae371,
			&["id", "namespace", "name", "value_type", "id_type"],
		),
		("dictionary::dictionary_namespace", &*DICTIONARY_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		("flow_edge::flow_edge", &*FLOW_EDGE_SHAPE, 0x4ba50fb1043e45f5, &["id", "flow", "source", "target"]),
		("flow_edge::flow_edge_by_flow", &*FLOW_EDGE_BY_FLOW_SHAPE, 0x511118ba66efb6ed, &["flow", "id"]),
		("flow::flow", &*FLOW_SHAPE, 0xe52fe38d6df347a1, &["id", "namespace", "name", "status", "reason"]),
		("flow::flow_namespace", &*FLOW_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		("granted_role::granted_role", &*GRANTED_ROLE_SHAPE, 0x6bc1886b8cfaa464, &["identity", "role_id"]),
		(
			"handler::handler",
			&*HANDLER_SHAPE,
			0x1a0132879eacab96,
			&["id", "namespace", "name", "on_sumtype_id", "on_variant_tag", "body_source"],
		),
		("handler::handler_namespace", &*HANDLER_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"identity_attribute::identity_attribute",
			&*IDENTITY_ATTRIBUTE_SHAPE,
			0x24580d0fa4179527,
			&["id", "name", "value_type"],
		),
		(
			"identity_attribute_value::identity_attribute_value",
			&*IDENTITY_ATTRIBUTE_VALUE_SHAPE,
			0x61a5b4645e0d8ee8,
			&["identity", "attribute", "value"],
		),
		("identity::identity", &*IDENTITY_SHAPE, 0x47f502a317cd4a61, &["identity", "name", "enabled", "kind"]),
		(
			"migration::migration",
			&*MIGRATION_SHAPE,
			0x86f1c05ec3364668,
			&["id", "name", "body", "rollback_body", "hash"],
		),
		(
			"migration::migration_event",
			&*MIGRATION_EVENT_SHAPE,
			0x1437e06604d16155,
			&["id", "migration_id", "action"],
		),
		(
			"namespace::namespace",
			&*NAMESPACE_SHAPE,
			0x9c6a56d9c54b58c3,
			&["id", "name", "parent_id", "grpc", "local_name", "token"],
		),
		(
			"operator_retention::operator_retention",
			&*OPERATOR_RETENTION_SHAPE,
			0xf8e6653f9f9c4f19,
			&["duration"],
		),
		("operator::operator", &*OPERATOR_SHAPE, 0xa93d5c45f91e4225, &["id", "flow", "type", "data"]),
		("operator::operator_by_flow", &*OPERATOR_BY_FLOW_SHAPE, 0x511118ba66efb6ed, &["flow", "id"]),
		(
			"policy::policy",
			&*POLICY_SHAPE,
			0xba67c6b2ac10a9d9,
			&["id", "name", "target_type", "target_namespace", "target_object", "enabled"],
		),
		(
			"policy::policy_op",
			&*POLICY_OP_SHAPE,
			0x31fc7d5d42e7bed1,
			&["policy_id", "operation", "body_source"],
		),
		("primary_key::primary_key", &*PRIMARY_KEY_SHAPE, 0x39d0e44addf6fe09, &["id", "source", "column_ids"]),
		(
			"procedure::procedure",
			&*PROCEDURE_SHAPE,
			0x34aaf2aedb0b0d99,
			&[
				"id",
				"namespace",
				"name",
				"variant",
				"body",
				"trigger_kind",
				"trigger_variant_sumtype",
				"trigger_variant_index",
				"return_type",
				"run_as",
			],
		),
		("procedure::namespace_procedure", &*NAMESPACE_PROCEDURE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"procedure::procedure_param",
			&*PROCEDURE_PARAM_SHAPE,
			0x9a8f6ccb6c1e2fba,
			&["procedure_id", "index", "name", "type_constraint"],
		),
		(
			"queue::queue",
			&*QUEUE_SHAPE,
			0x40281c3e3664af79,
			&[
				"id",
				"namespace",
				"name",
				"partition_by",
				"ordered_by",
				"retention_done",
				"retry_attempts",
				"retry_backoff",
				"deduplicate_by",
				"deduplicate_ttl",
				"dispatch",
				"ts",
				"time_domain",
			],
		),
		("queue::queue_namespace", &*QUEUE_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"relationship::relationship",
			&*RELATIONSHIP_SHAPE,
			0x89a86deee1d0b810,
			&[
				"id",
				"namespace_id",
				"name",
				"source_table_id",
				"source_column_id",
				"target_table_id",
				"target_column_id",
				"junction_table_id",
				"junction_source_column_id",
				"junction_target_column_id",
				"cardinality",
			],
		),
		(
			"ringbuffer::ringbuffer",
			&*RINGBUFFER_SHAPE,
			0x42f90939729c56cb,
			&["id", "namespace", "name", "capacity", "primary_key", "partition_by", "ts", "time_domain"],
		),
		("ringbuffer::ringbuffer_namespace", &*RINGBUFFER_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		("role::role", &*ROLE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		("row_settings::row_settings", &*ROW_SETTINGS_SHAPE, 0x7798573f24eddf53, &["duration", "persistent"]),
		(
			"row_shape::shape_field",
			&*SHAPE_FIELD_SHAPE,
			0x0e785ed9dfee4ada,
			&["name", "type_constraint", "offset", "size"],
		),
		(
			"series::series",
			&*SERIES_SHAPE,
			0x51234689f486f11d,
			&[
				"id",
				"namespace",
				"name",
				"tag",
				"key_column",
				"key_kind",
				"precision",
				"primary_key",
				"partition_by",
				"ts",
				"time_domain",
				"encoding_name",
				"encoding_config",
			],
		),
		("series::series_namespace", &*SERIES_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"sink::sink",
			&*SINK_SHAPE,
			0x8660efef3c6a9005,
			&["id", "namespace", "name", "source_namespace", "source_name", "connector", "config"],
		),
		("sink::sink_namespace", &*SINK_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"source::source",
			&*SOURCE_SHAPE,
			0x29016a951acf4bd3,
			&["id", "namespace", "name", "connector", "config", "target_namespace", "target_name"],
		),
		("source::source_namespace", &*SOURCE_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"sumtype::sumtype",
			&*SUMTYPE_SHAPE,
			0x9b7790b5d1541b01,
			&["id", "namespace", "name", "variants_json", "kind"],
		),
		("sumtype::sumtype_namespace", &*SUMTYPE_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"table::table",
			&*TABLE_SHAPE,
			0xa25e438ef3e2e1de,
			&["id", "namespace", "name", "primary_key", "partition_by", "ts", "time_domain"],
		),
		("table::table_namespace", &*TABLE_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"token::token",
			&*TOKEN_SHAPE,
			0x40c303a7adf7386e,
			&["id", "token", "identity", "expires_at", "created_at", "kind"],
		),
		(
			"view::view",
			&*VIEW_SHAPE,
			0x55a46edaba9bc51a,
			&[
				"id",
				"namespace",
				"name",
				"kind",
				"primary_key",
				"storage_kind",
				"capacity",
				"key_column",
				"key_kind",
				"precision",
				"tag_id",
				"partition_by",
				"sort",
			],
		),
		("view::view_namespace", &*VIEW_NAMESPACE_SHAPE, 0x4ba761281fac062e, &["id", "name"]),
		(
			"windowed_view_group::windowed_view_group",
			&*WINDOWED_VIEW_GROUP_SHAPE,
			0xf0a0f692ba7625ea,
			&["id", "namespace", "name", "flow", "members"],
		),
		(
			"windowed_view_group::windowed_view_group_namespace",
			&*WINDOWED_VIEW_GROUP_NAMESPACE_SHAPE,
			0x4ba761281fac062e,
			&["id", "name"],
		),
	]
}

#[test]
fn every_catalog_shape_matches_its_pinned_fingerprint() {
	for (name, shape, fingerprint, fields) in pinned() {
		let actual: Vec<&str> = shape.fields().iter().map(|f| f.name.as_str()).collect();
		assert_eq!(actual, fields, "field list drifted for {name}");
		assert_eq!(
			*shape.fingerprint(),
			fingerprint,
			"fingerprint drifted for {name}: a shape rewrite must reproduce names, types, constraints and order exactly"
		);
	}
}

#[test]
fn the_pin_table_covers_every_catalog_shape() {
	assert_eq!(
		pinned().len(),
		shapes_declared_on_disk(),
		"a catalog shape was added or removed without updating the pin table, so the rewrite guard no longer covers it"
	);
}

#[test]
fn structurally_identical_shapes_share_one_fingerprint() {
	let id_name: Vec<&str> =
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0x4ba761281fac062e).map(|(n, ..)| *n).collect();
	assert_eq!(id_name.len(), 15, "the id/name index shapes must stay interchangeable by fingerprint");

	let flow_id: Vec<&str> =
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0x511118ba66efb6ed).map(|(n, ..)| *n).collect();
	assert_eq!(flow_id.len(), 2, "the flow/id index shapes must stay interchangeable by fingerprint");
}
