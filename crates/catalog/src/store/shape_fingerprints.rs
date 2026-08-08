// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, path::Path};

use reifydb_codec::row::shape::RowShape;

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
			&*crate::store::authentication::shape::authentication::SHAPE,
			0x069e73862d6297ed,
			&["id", "identity", "method", "properties"],
		),
		(
			"binding::binding",
			&*crate::store::binding::shape::binding::SHAPE,
			0xff095d2b5d3cdd1c,
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
		(
			"binding::binding_namespace",
			&*crate::store::binding::shape::binding_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"column_property::column_property",
			&*crate::store::column_property::shape::column_property::SHAPE,
			0x0c19e4cad0c599fb,
			&["id", "column", "policy", "value"],
		),
		(
			"column::column",
			&*crate::store::column::shape::column::SHAPE,
			0x86fea33e630b6d99,
			&["id", "object", "name", "value", "index", "auto_increment", "constraint", "dictionary_id"],
		),
		(
			"column::object_column",
			&*crate::store::column::shape::object_column::SHAPE,
			0x3e44f49bed486d31,
			&["id", "name", "index"],
		),
		(
			"column_snapshot::column_snapshot",
			&*crate::store::column_snapshot::shape::column_snapshot::SHAPE,
			0x9e9f6958cf2d14eb,
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
		(
			"column_snapshot::column_snapshot_link",
			&*crate::store::column_snapshot::shape::column_snapshot_link::SHAPE,
			0x1d9677b49fb39c2c,
			&["id"],
		),
		("config::config", &*crate::store::config::shape::config::SHAPE, 0x968426172f0419b7, &["value"]),
		(
			"dictionary::dictionary",
			&*crate::store::dictionary::shape::dictionary::SHAPE,
			0xb53e22f70ef9c3c8,
			&["id", "namespace", "name", "value_type", "id_type"],
		),
		(
			"dictionary::dictionary_namespace",
			&*crate::store::dictionary::shape::dictionary_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"flow_edge::flow_edge",
			&*crate::store::flow_edge::shape::flow_edge::SHAPE,
			0xacdd49bb75b820cc,
			&["id", "flow", "source", "target"],
		),
		(
			"flow_edge::flow_edge_by_flow",
			&*crate::store::flow_edge::shape::flow_edge_by_flow::SHAPE,
			0xed84d4eb2056758c,
			&["flow", "id"],
		),
		(
			"flow::flow",
			&*crate::store::flow::shape::flow::SHAPE,
			0xd6831c454a16ad5e,
			&["id", "namespace", "name", "status"],
		),
		(
			"flow::flow_namespace",
			&*crate::store::flow::shape::flow_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"granted_role::granted_role",
			&*crate::store::granted_role::shape::granted_role::SHAPE,
			0x6e4919ac03bf3707,
			&["identity", "role_id"],
		),
		(
			"handler::handler",
			&*crate::store::handler::shape::handler::SHAPE,
			0xab6f6fbe2d67b4e5,
			&["id", "namespace", "name", "on_sumtype_id", "on_variant_tag", "body_source"],
		),
		(
			"handler::handler_namespace",
			&*crate::store::handler::shape::handler_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"identity_attribute::identity_attribute",
			&*crate::store::identity_attribute::shape::identity_attribute::SHAPE,
			0x466fb58c86fea7a8,
			&["id", "name", "value_type"],
		),
		(
			"identity_attribute_value::identity_attribute_value",
			&*crate::store::identity_attribute_value::shape::identity_attribute_value::SHAPE,
			0x71b5e4cee745f8e3,
			&["identity", "attribute", "value"],
		),
		(
			"identity::identity",
			&*crate::store::identity::shape::identity::SHAPE,
			0x691e5810139ac9fe,
			&["identity", "name", "enabled"],
		),
		(
			"migration::migration",
			&*crate::store::migration::shape::migration::SHAPE,
			0x52b88abddcf02501,
			&["id", "name", "body", "rollback_body", "hash"],
		),
		(
			"migration::migration_event",
			&*crate::store::migration::shape::migration_event::SHAPE,
			0xff334b148eb8f7f6,
			&["id", "migration_id", "action"],
		),
		(
			"namespace::namespace",
			&*crate::store::namespace::shape::namespace::SHAPE,
			0x0117aa41381a8e9b,
			&["id", "name", "parent_id", "grpc", "local_name", "token"],
		),
		(
			"operator_settings::operator_settings",
			&*crate::store::operator_settings::shape::operator_settings::SHAPE,
			0x30bf90e5b579eb35,
			&["is_join", "duration", "left_duration", "right_duration"],
		),
		(
			"operator::operator",
			&*crate::store::operator::shape::operator::SHAPE,
			0xc3a38db2c6c37321,
			&["id", "flow", "type", "data"],
		),
		(
			"operator::operator_by_flow",
			&*crate::store::operator::shape::operator_by_flow::SHAPE,
			0xed84d4eb2056758c,
			&["flow", "id"],
		),
		(
			"policy::policy",
			&*crate::store::policy::shape::policy::SHAPE,
			0x0422a0aaaeef78ea,
			&["id", "name", "target_type", "target_namespace", "target_object", "enabled"],
		),
		(
			"policy::policy_op",
			&*crate::store::policy::shape::policy_op::SHAPE,
			0xeecfab9afb6b37a2,
			&["policy_id", "operation", "body_source"],
		),
		(
			"primary_key::primary_key",
			&*crate::store::primary_key::shape::primary_key::SHAPE,
			0x4b8a5cb73c02ca90,
			&["id", "source", "column_ids"],
		),
		(
			"procedure::procedure",
			&*crate::store::procedure::shape::procedure::SHAPE,
			0xf444dfa78802210d,
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
		(
			"procedure::namespace_procedure",
			&*crate::store::procedure::shape::namespace_procedure::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"procedure::procedure_param",
			&*crate::store::procedure::shape::procedure_param::SHAPE,
			0x32512449912c3927,
			&["procedure_id", "index", "name", "type_constraint"],
		),
		(
			"queue::queue",
			&*crate::store::queue::shape::queue::SHAPE,
			0xccf4c8e782a1a9f6,
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
		(
			"queue::queue_namespace",
			&*crate::store::queue::shape::queue_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"relationship::relationship",
			&*crate::store::relationship::shape::relationship::SHAPE,
			0x267b5b73b2c0307b,
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
			&*crate::store::ringbuffer::shape::ringbuffer::SHAPE,
			0x80e10f21e4ca3c4a,
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
		(
			"ringbuffer::ringbuffer_namespace",
			&*crate::store::ringbuffer::shape::ringbuffer_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		("role::role", &*crate::store::role::shape::role::SHAPE, 0x43dc722774b6e165, &["id", "name"]),
		(
			"row_settings::row_settings",
			&*crate::store::row_settings::shape::row_settings::SHAPE,
			0x18e4f596dd5972f1,
			&["announce", "duration", "persistent"],
		),
		(
			"row_shape::shape_header",
			&*crate::store::row_shape::shape::shape_header::SHAPE,
			0x57677554e4d3694e,
			&["field_count"],
		),
		(
			"row_shape::shape_field",
			&*crate::store::row_shape::shape::shape_field::SHAPE,
			0x3188ba30e87320b6,
			&[
				"name",
				"base_type",
				"constraint_type",
				"constraint_p1",
				"constraint_p2",
				"offset",
				"size",
				"align",
			],
		),
		(
			"sequence::sequence",
			&*crate::store::sequence::shape::sequence::SHAPE,
			0xf6bcd29d9797c44a,
			&["value"],
		),
		(
			"series::series",
			&*crate::store::series::shape::series::SHAPE,
			0xa9ca38a7b0557f86,
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
		(
			"series::series_namespace",
			&*crate::store::series::shape::series_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"series::series_metadata",
			&*crate::store::series::shape::series_metadata::SHAPE,
			0xbc08badca0dd4421,
			&["id", "row_count", "oldest_key", "newest_key", "sequence_counter"],
		),
		(
			"sink::sink",
			&*crate::store::sink::shape::sink::SHAPE,
			0xe40347f64ac0fafb,
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
		(
			"sink::sink_namespace",
			&*crate::store::sink::shape::sink_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"source::source",
			&*crate::store::source::shape::source::SHAPE,
			0x44bb1cbbe666e9a2,
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
		(
			"source::source_namespace",
			&*crate::store::source::shape::source_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"sumtype::sumtype",
			&*crate::store::sumtype::shape::sumtype::SHAPE,
			0x58a5744edf0a46f5,
			&["id", "namespace", "name", "variants_json", "kind"],
		),
		(
			"sumtype::sumtype_namespace",
			&*crate::store::sumtype::shape::sumtype_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"table::table",
			&*crate::store::table::shape::table::SHAPE,
			0xd9bbf82b8e90d263,
			&["id", "namespace", "name", "primary_key", "partition_by", "underlying", "ts", "time_domain"],
		),
		(
			"table::table_namespace",
			&*crate::store::table::shape::table_namespace::SHAPE,
			0x43dc722774b6e165,
			&["id", "name"],
		),
		(
			"token::token",
			&*crate::store::token::shape::token::SHAPE,
			0xfa9473456433f2c7,
			&["id", "token", "identity", "expires_at", "created_at"],
		),
		(
			"view::view",
			&*crate::store::view::shape::view::SHAPE,
			0x42bdb56e655761d6,
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
		(
			"view::view_namespace",
			&*crate::store::view::shape::view_namespace::SHAPE,
			0x43dc722774b6e165,
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
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0x43dc722774b6e165).map(|(n, ..)| *n).collect();
	assert_eq!(id_name.len(), 14, "the id/name index shapes must stay interchangeable by fingerprint");

	let flow_id: Vec<&str> =
		pinned().iter().filter(|(_, _, fp, _)| *fp == 0xed84d4eb2056758c).map(|(n, ..)| *n).collect();
	assert_eq!(flow_id.len(), 2, "the flow/id index shapes must stay interchangeable by fingerprint");
}
