// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, path::Path};

use reifydb_codec::row::shape::RowShape;

use crate::store::{
	authentication::shape::authentication::SHAPE as AUTHENTICATION_SHAPE,
	binding::shape::{binding::SHAPE as BINDING_SHAPE, binding_namespace::SHAPE as BINDING_NAMESPACE_SHAPE},
	column::shape::{column::SHAPE as COLUMN_SHAPE, object_column::SHAPE as OBJECT_COLUMN_SHAPE},
	column_property::shape::column_property::SHAPE as COLUMN_PROPERTY_SHAPE,
	column_snapshot::shape::column_snapshot::SHAPE as COLUMN_SNAPSHOT_SHAPE,
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
	operator_settings::shape::operator_settings::SHAPE as OPERATOR_SETTINGS_SHAPE,
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
			0x348d84f39b551800,
			&["id", "identity", "method", "properties"],
		),
		(
			"binding::binding",
			&*BINDING_SHAPE,
			0x694b474c52fc9ba2,
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
		("binding::binding_namespace", &*BINDING_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"column_property::column_property",
			&*COLUMN_PROPERTY_SHAPE,
			0x2bfd8568eb0bff3f,
			&["id", "column", "policy", "value"],
		),
		(
			"column::column",
			&*COLUMN_SHAPE,
			0xebe75c523479eab9,
			&["id", "object", "name", "value", "index", "auto_increment", "constraint", "dictionary_id"],
		),
		("column::object_column", &*OBJECT_COLUMN_SHAPE, 0x0c2cd6dc1e3f9c7a, &["id", "name", "index"]),
		(
			"column_snapshot::column_snapshot",
			&*COLUMN_SNAPSHOT_SHAPE,
			0x2864525861aa7c41,
			&[
				"id",
				"namespace",
				"kind",
				"source_id",
				"bucket_start",
				"bucket_width",
				"sequence_counter",
				"read_version",
				"row_count",
			],
		),
		("config::config", &*CONFIG_SHAPE, 0xefc802db77165323, &["value"]),
		(
			"dictionary::dictionary",
			&*DICTIONARY_SHAPE,
			0xaae7182a0136c3fe,
			&["id", "namespace", "name", "value_type", "id_type"],
		),
		("dictionary::dictionary_namespace", &*DICTIONARY_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		("flow_edge::flow_edge", &*FLOW_EDGE_SHAPE, 0xc1e33b21552a7666, &["id", "flow", "source", "target"]),
		("flow_edge::flow_edge_by_flow", &*FLOW_EDGE_BY_FLOW_SHAPE, 0x9486e81a3cbbeed4, &["flow", "id"]),
		("flow::flow", &*FLOW_SHAPE, 0x6f6fa7840877d5a4, &["id", "namespace", "name", "status"]),
		("flow::flow_namespace", &*FLOW_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		("granted_role::granted_role", &*GRANTED_ROLE_SHAPE, 0xc03109d375f3c585, &["identity", "role_id"]),
		(
			"handler::handler",
			&*HANDLER_SHAPE,
			0x600d014356243a4b,
			&["id", "namespace", "name", "on_sumtype_id", "on_variant_tag", "body_source"],
		),
		("handler::handler_namespace", &*HANDLER_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"identity_attribute::identity_attribute",
			&*IDENTITY_ATTRIBUTE_SHAPE,
			0x46281ccd82110d21,
			&["id", "name", "value_type"],
		),
		(
			"identity_attribute_value::identity_attribute_value",
			&*IDENTITY_ATTRIBUTE_VALUE_SHAPE,
			0x0d4888ba97084b01,
			&["identity", "attribute", "value"],
		),
		("identity::identity", &*IDENTITY_SHAPE, 0xedf855f31f733b03, &["identity", "name", "enabled"]),
		(
			"migration::migration",
			&*MIGRATION_SHAPE,
			0x585864c1dc20f7a0,
			&["id", "name", "body", "rollback_body", "hash"],
		),
		(
			"migration::migration_event",
			&*MIGRATION_EVENT_SHAPE,
			0x64877986abbbc1e2,
			&["id", "migration_id", "action"],
		),
		(
			"namespace::namespace",
			&*NAMESPACE_SHAPE,
			0xded5f2ae8d70fea5,
			&["id", "name", "parent_id", "grpc", "local_name", "token"],
		),
		(
			"operator_settings::operator_settings",
			&*OPERATOR_SETTINGS_SHAPE,
			0xbb49667b475c8594,
			&["is_join", "duration", "left_duration", "right_duration"],
		),
		("operator::operator", &*OPERATOR_SHAPE, 0x7a8cd42d0f4b6e96, &["id", "flow", "type", "data"]),
		("operator::operator_by_flow", &*OPERATOR_BY_FLOW_SHAPE, 0x9486e81a3cbbeed4, &["flow", "id"]),
		(
			"policy::policy",
			&*POLICY_SHAPE,
			0x2d8044426d5d0dba,
			&["id", "name", "target_type", "target_namespace", "target_object", "enabled"],
		),
		(
			"policy::policy_op",
			&*POLICY_OP_SHAPE,
			0x11dcb638f55cbf27,
			&["policy_id", "operation", "body_source"],
		),
		("primary_key::primary_key", &*PRIMARY_KEY_SHAPE, 0xb89cc69a5a765085, &["id", "source", "column_ids"]),
		(
			"procedure::procedure",
			&*PROCEDURE_SHAPE,
			0x0e6d0dd0563391fb,
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
			],
		),
		("procedure::namespace_procedure", &*NAMESPACE_PROCEDURE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"procedure::procedure_param",
			&*PROCEDURE_PARAM_SHAPE,
			0x92be3c99eaa34ca8,
			&["procedure_id", "index", "name", "type_constraint"],
		),
		(
			"queue::queue",
			&*QUEUE_SHAPE,
			0xe841f66c7b2d9a43,
			&[
				"id",
				"namespace",
				"name",
				"partitions",
				"ordered_by",
				"retention_done",
				"retry_attempts",
				"retry_backoff",
				"underlying",
				"deduplicate_by",
				"deduplicate_ttl",
				"dispatch",
				"ts",
				"time_domain",
			],
		),
		("queue::queue_namespace", &*QUEUE_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"relationship::relationship",
			&*RELATIONSHIP_SHAPE,
			0x5ad9666f4e6d2f70,
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
			0x3092993e516b3072,
			&[
				"id",
				"namespace",
				"name",
				"capacity",
				"primary_key",
				"partition_by",
				"underlying",
				"ts",
				"time_domain",
			],
		),
		("ringbuffer::ringbuffer_namespace", &*RINGBUFFER_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		("role::role", &*ROLE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		("row_settings::row_settings", &*ROW_SETTINGS_SHAPE, 0x7a5ec2164996219f, &["duration", "persistent"]),
		(
			"row_shape::shape_field",
			&*SHAPE_FIELD_SHAPE,
			0xb9845fd1335edcbd,
			&["name", "base_type", "constraint_type", "constraint_p1", "constraint_p2", "offset", "size"],
		),
		(
			"series::series",
			&*SERIES_SHAPE,
			0x2b3867e449ca3245,
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
				"underlying",
				"ts",
				"time_domain",
			],
		),
		("series::series_namespace", &*SERIES_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"sink::sink",
			&*SINK_SHAPE,
			0x23ce8d4d52c47d9b,
			&[
				"id",
				"namespace",
				"name",
				"source_namespace",
				"source_name",
				"connector",
				"config",
				"status",
			],
		),
		("sink::sink_namespace", &*SINK_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"source::source",
			&*SOURCE_SHAPE,
			0xabf2c6378d530295,
			&[
				"id",
				"namespace",
				"name",
				"connector",
				"config",
				"target_namespace",
				"target_name",
				"status",
			],
		),
		("source::source_namespace", &*SOURCE_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"sumtype::sumtype",
			&*SUMTYPE_SHAPE,
			0x2d33916420752127,
			&["id", "namespace", "name", "variants_json", "kind"],
		),
		("sumtype::sumtype_namespace", &*SUMTYPE_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"table::table",
			&*TABLE_SHAPE,
			0x15de9ff7b8439d89,
			&["id", "namespace", "name", "primary_key", "partition_by", "underlying", "ts", "time_domain"],
		),
		("table::table_namespace", &*TABLE_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
		(
			"token::token",
			&*TOKEN_SHAPE,
			0x9587a04c89ad35cc,
			&["id", "token", "identity", "expires_at", "created_at"],
		),
		(
			"view::view",
			&*VIEW_SHAPE,
			0x1d3d198ba07ad900,
			&[
				"id",
				"namespace",
				"name",
				"kind",
				"primary_key",
				"storage_kind",
				"storage_id",
				"capacity",
				"key_column",
				"key_kind",
				"precision",
				"tag_id",
				"sort",
			],
		),
		("view::view_namespace", &*VIEW_NAMESPACE_SHAPE, 0x6ac7ea1f39781c6f, &["id", "name"]),
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
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0x6ac7ea1f39781c6f).map(|(n, ..)| *n).collect();
	assert_eq!(id_name.len(), 14, "the id/name index shapes must stay interchangeable by fingerprint");

	let flow_id: Vec<&str> =
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0x9486e81a3cbbeed4).map(|(n, ..)| *n).collect();
	assert_eq!(flow_id.len(), 2, "the flow/id index shapes must stay interchangeable by fingerprint");
}
