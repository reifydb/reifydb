// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::key::{KeyColumn, KeyField};

#[derive(Clone, Copy, PartialEq)]
pub enum Direction {
	Asc,
	Desc,
}

pub struct OrderComponent {
	projection: &'static str,
	direction: Direction,
}

impl KeyColumn {
	pub fn samples(self, ty: &str) -> String {
		match self {
			KeyColumn::U8 => "vec![0u8, 1u8, u8::MAX - 1, u8::MAX]".to_string(),
			KeyColumn::U16 => "vec![0u16, 1u16, u16::MAX - 1, u16::MAX]".to_string(),
			KeyColumn::U32 => "vec![0u32, 1u32, u32::MAX - 1, u32::MAX]".to_string(),
			KeyColumn::U64 => "vec![0u64, 1u64, u64::MAX - 1, u64::MAX]".to_string(),
			KeyColumn::U128 => "vec![0u128, 1u128, u128::MAX - 1, u128::MAX]".to_string(),
			KeyColumn::ReprU8 => {
				format!("{{ let mut __v = Vec::new(); for __b in 0u8..=u8::MAX {{ if let Ok(__x) = \
				 {ty}::try_from(__b) {{ __v.push(__x); }} }} __v }}")
			}
			KeyColumn::RowNumber => {
				"vec![RowNumber(0), RowNumber(1), RowNumber(u64::MAX - 1), RowNumber(u64::MAX)]"
					.to_string()
			}
			KeyColumn::GroupId => "vec![GroupId::from_bytes([0u8; 24]), GroupId::from_bytes([1u8; 24]), \
				GroupId::from_bytes([254u8; 24]), GroupId::from_bytes([255u8; 24])]"
				.to_string(),
			KeyColumn::Blob16 => "vec![[0u8; 16], [1u8; 16], [254u8; 16], [255u8; 16]]".to_string(),
			KeyColumn::IdentityId => "vec![IdentityId::default(), IdentityId::anonymous(), \
				IdentityId::system(), IdentityId::root()]"
				.to_string(),
			KeyColumn::ObjectId => "{ let mut __v = Vec::new(); for __tag in 0u8..=u8::MAX { for __id in \
				[0u64, 1u64, u64::MAX - 1, u64::MAX] { if let Some(__o) = \
				ObjectId::from_type_tag(__tag, __id) { __v.push(__o); } } } __v }"
				.to_string(),
			KeyColumn::StorageId => "{ let mut __v = Vec::new(); for __tag in 0u8..=u8::MAX { for __id in \
				[0u64, 1u64, u64::MAX - 1, u64::MAX] { if let Some(__o) = \
				StorageId::from_type_tag(__tag, __id) { __v.push(__o); } } } __v }"
				.to_string(),
			KeyColumn::ProcedureId => "vec![ProcedureId::from_raw(0), ProcedureId::from_raw(1), \
				ProcedureId::from_raw(u64::MAX - 1), ProcedureId::from_raw(u64::MAX)]"
				.to_string(),
			KeyColumn::EpochSeconds => "vec![EpochSeconds::new(0), EpochSeconds::new(1), \
				EpochSeconds::new(u64::MAX - 1), EpochSeconds::new(u64::MAX)]"
				.to_string(),
			KeyColumn::DateTime => "vec![DateTime::from_nanos(0), DateTime::from_nanos(1), \
				DateTime::from_nanos(u64::MAX - 1), DateTime::from_nanos(u64::MAX)]"
				.to_string(),
			KeyColumn::RowShapeFingerprint => {
				"vec![RowShapeFingerprint::new(0), RowShapeFingerprint::new(1), \
				 RowShapeFingerprint::new(u64::MAX - 1), RowShapeFingerprint::new(u64::MAX)]"
					.to_string()
			}
			KeyColumn::Partition => {
				"vec![Partition(0), Partition(1), Partition(u128::MAX - 1), Partition(u128::MAX)]"
					.to_string()
			}
			KeyColumn::IndexId => "vec![IndexId::primary(0u64), IndexId::primary(1u64), \
				IndexId::primary(u64::MAX - 1), IndexId::primary(u64::MAX)]"
				.to_string(),
			KeyColumn::OptionU8 => "vec![None, Some(0u8), Some(1u8), Some(254u8), Some(255u8)]".to_string(),
			KeyColumn::TableId
			| KeyColumn::ColumnId
			| KeyColumn::FlowId
			| KeyColumn::FlowEdgeId
			| KeyColumn::OperatorId
			| KeyColumn::HandlerId
			| KeyColumn::NamespaceId
			| KeyColumn::SumTypeId
			| KeyColumn::SequenceId
			| KeyColumn::ViewId
			| KeyColumn::SeriesId
			| KeyColumn::SinkId
			| KeyColumn::SourceId
			| KeyColumn::QueueId
			| KeyColumn::RingBufferId
			| KeyColumn::BindingId
			| KeyColumn::DictionaryId
			| KeyColumn::ColumnPropertyId
			| KeyColumn::RelationshipId
			| KeyColumn::MigrationId
			| KeyColumn::MigrationEventId
			| KeyColumn::ColumnSnapshotId
			| KeyColumn::PrimaryKeyId => {
				format!("vec![{ty}(0), {ty}(1), {ty}(u64::MAX - 1), {ty}(u64::MAX)]")
			}
		}
	}

	pub fn order(self) -> Vec<OrderComponent> {
		let desc = |projection| OrderComponent {
			projection,
			direction: Direction::Desc,
		};
		let asc = |projection| OrderComponent {
			projection,
			direction: Direction::Asc,
		};
		match self {
			KeyColumn::U8 | KeyColumn::U16 | KeyColumn::U32 | KeyColumn::U64 | KeyColumn::U128 => {
				vec![desc("$")]
			}
			KeyColumn::ReprU8 => vec![desc("($ as u8)")],
			KeyColumn::GroupId | KeyColumn::IdentityId => vec![desc("(*$.as_bytes())")],
			KeyColumn::Blob16 => vec![asc("$")],
			KeyColumn::ObjectId | KeyColumn::StorageId => {
				vec![asc("$.type_tag()"), desc("$.as_u64()")]
			}
			KeyColumn::OptionU8 => vec![desc("$.is_some()"), desc("$.unwrap_or(0u8)")],
			KeyColumn::ProcedureId => vec![desc("(*$)")],
			KeyColumn::EpochSeconds => vec![desc("$.seconds()")],
			KeyColumn::DateTime => vec![desc("$.to_nanos()")],
			KeyColumn::RowShapeFingerprint | KeyColumn::IndexId => vec![desc("$.as_u64()")],
			KeyColumn::RowNumber | KeyColumn::Partition => vec![desc("$.0")],
			KeyColumn::TableId
			| KeyColumn::ColumnId
			| KeyColumn::FlowId
			| KeyColumn::FlowEdgeId
			| KeyColumn::OperatorId
			| KeyColumn::HandlerId
			| KeyColumn::NamespaceId
			| KeyColumn::SumTypeId
			| KeyColumn::SequenceId
			| KeyColumn::ViewId
			| KeyColumn::SeriesId
			| KeyColumn::SinkId
			| KeyColumn::SourceId
			| KeyColumn::QueueId
			| KeyColumn::RingBufferId
			| KeyColumn::BindingId
			| KeyColumn::DictionaryId
			| KeyColumn::ColumnPropertyId
			| KeyColumn::RelationshipId
			| KeyColumn::MigrationId
			| KeyColumn::MigrationEventId
			| KeyColumn::ColumnSnapshotId
			| KeyColumn::PrimaryKeyId => vec![desc("$.0")],
		}
	}
}

pub fn expand_tests(name: &str, fields: &[KeyField]) -> String {
	let mut out = String::new();

	out.push_str("#[cfg(test)]\n#[allow(non_snake_case, unused_imports, unused_variables)]\n");
	out.push_str(&format!("mod __key_conformance_{name} {{\n"));
	out.push_str("\tuse std::cmp::Ordering;\n\n");
	out.push_str("\tuse super::*;\n\n");

	out.push_str(&samples_fn(name, fields));
	out.push('\n');
	out.push_str(&expected_order_fn(name, fields));
	out.push('\n');
	out.push_str(&round_trip_test(name));
	out.push('\n');
	out.push_str(&order_test(name));
	out.push('\n');
	out.push_str(&byte_replay_test(name));

	out.push_str("}\n");
	out
}

fn samples_fn(name: &str, fields: &[KeyField]) -> String {
	let mut out = String::new();
	out.push_str(&format!("\tfn __samples() -> Vec<{name}> {{\n"));

	for field in fields {
		out.push_str(&format!(
			"\t\tlet __f_{}: Vec<{}> = {};\n\t\tassert!(!__f_{}.is_empty(), \"column '{}' of \
			 {name} produced no boundary values to compare\");\n",
			field.name,
			field.ty,
			field.column.samples(&field.ty),
			field.name,
			field.name
		));
	}

	out.push_str(&format!("\t\tlet mut __out: Vec<{name}> = Vec::new();\n"));

	if fields.is_empty() {
		out.push_str(&format!("\t\t__out.push({name} {{}});\n"));
	} else {
		for (index, field) in fields.iter().enumerate() {
			let skip = if index == 0 {
				""
			} else {
				".skip(1)"
			};
			out.push_str(&format!("\t\tfor __v in __f_{}.iter(){skip} {{\n", field.name));
			out.push_str(&format!("\t\t\t__out.push({name} {{\n"));
			for other in fields {
				if other.name == field.name {
					out.push_str(&format!("\t\t\t\t{}: __v.clone(),\n", other.name));
				} else {
					out.push_str(&format!(
						"\t\t\t\t{}: __f_{}[0].clone(),\n",
						other.name, other.name
					));
				}
			}
			out.push_str("\t\t\t});\n\t\t}\n");
		}
	}

	out.push_str("\t\t__out\n\t}\n");
	out
}

fn expected_order_fn(name: &str, fields: &[KeyField]) -> String {
	let mut out = String::new();
	out.push_str(&format!("\tfn __expected_order(__a: &{name}, __b: &{name}) -> (Ordering, &'static str) {{\n"));

	for field in fields {
		for component in field.column.order() {
			let left = component.projection.replace('$', &format!("__a.{}", field.name));
			let right = component.projection.replace('$', &format!("__b.{}", field.name));
			let compared = match component.direction {
				Direction::Asc => format!("{left}.cmp(&{right})"),
				Direction::Desc => format!("{right}.cmp(&{left})"),
			};
			out.push_str(&format!("\t\tlet __ord = {compared};\n"));
			out.push_str(&format!(
				"\t\tif __ord != Ordering::Equal {{\n\t\t\treturn (__ord, \"{}\");\n\t\t}}\n",
				field.name
			));
		}
	}

	out.push_str("\t\t(Ordering::Equal, \"<no column>\")\n\t}\n");
	out
}

fn round_trip_test(name: &str) -> String {
	let mut out = String::new();
	out.push_str("\t// a key that cannot be read back has silently lost a column somewhere in its bytes.\n");
	out.push_str("\t#[test]\n\tfn every_boundary_instance_decodes_back_to_itself() {\n");
	out.push_str("\t\tfor __key in __samples().iter() {\n");
	out.push_str(&format!("\t\t\tlet __encoded = <{name} as Key>::encode(__key);\n"));
	out.push_str(&format!("\t\t\tlet __decoded = <{name} as Key>::decode(&__encoded);\n"));
	out.push_str(&format!(
		"\t\t\tassert_eq!(\n\t\t\t\t__decoded.as_ref(),\n\t\t\t\tSome(__key),\n\t\t\t\t\"{name} did not \
		 survive a round trip: {{:?}} encoded to {{:?}}\",\n\t\t\t\t__key,\n\t\t\t\t\
		 __encoded.as_slice()\n\t\t\t);\n"
	));
	out.push_str("\t\t}\n\t}\n");
	out
}

fn byte_replay_test(name: &str) -> String {
	let mut out = String::new();
	out.push_str(
		"\t// a projection that sorts right can still write the wrong bytes: fixed and escaped byte\n\t		 // encodings sort alike, and every integer width compares the same way. Only replaying the\n\t		 // bytes catches an encoding tag that does not match the column's serializer call.\n",
	);
	out.push_str("\t#[test]\n\tfn every_boundary_instance_replays_its_encoded_bytes_through_fields() {\n");
	out.push_str("\t\tfor __key in __samples().iter() {\n");
	out.push_str(&format!("\t\t\tlet mut __replayed = vec![!(<{name} as Key>::KIND as u8)];\n"));
	out.push_str("\t\t\tfor __field in __key.fields().iter() {\n");
	out.push_str("\t\t\t\t__field.encode(&mut __replayed);\n\t\t\t}\n");
	out.push_str(&format!(
		"\t\t\tassert_eq!(\n\t\t\t\t__replayed.as_slice(),\n\t\t\t\t<{name} as 		 Key>::encode(__key).as_slice(),\n\t\t\t\t\"{name}: fields() does not replay the bytes its 		 encoder wrote for {{:?}}\",\n\t\t\t\t__key\n\t\t\t);\n"
	));
	out.push_str("\t\t}\n\t}\n");
	out
}

fn order_test(name: &str) -> String {
	let mut out = String::new();
	out.push_str("\t// typed keys will replace byte keys in program logic, so both must sort alike.\n");
	out.push_str("\t#[test]\n\tfn encoded_byte_order_matches_the_declared_column_directions() {\n");
	out.push_str("\t\tlet __samples = __samples();\n");
	out.push_str(&format!("\t\tassert!(!__samples.is_empty(), \"{name} produced no instances to compare\");\n"));
	out.push_str("\t\tfor __a in __samples.iter() {\n\t\t\tfor __b in __samples.iter() {\n");
	out.push_str("\t\t\t\tlet (__expected, __column) = __expected_order(__a, __b);\n");
	out.push_str(&format!("\t\t\t\tlet __ea = <{name} as Key>::encode(__a);\n"));
	out.push_str(&format!("\t\t\t\tlet __eb = <{name} as Key>::encode(__b);\n"));
	out.push_str("\t\t\t\tlet __actual = __ea.as_slice().cmp(__eb.as_slice());\n");
	out.push_str(
		"\t\t\t\tassert_eq!(\n\t\t\t\t\t__expected,\n\t\t\t\t\t__expected_order(__b, \
		 __a).0.reverse(),\n\t\t\t\t\t\"the declared direction of column '{}' is not antisymmetric, so it \
		 cannot be a total order\",\n\t\t\t\t\t__column\n\t\t\t\t);\n",
	);
	out.push_str(&format!(
		"\t\t\t\tassert_eq!(\n\t\t\t\t\t__actual,\n\t\t\t\t\t__expected,\n\t\t\t\t\t\"{name}: encoded byte \
		 order disagrees with the declared direction of column '{{}}'\\n  a = {{:?}}\\n  b = {{:?}}\\n  a \
		 bytes = {{:?}}\\n  b bytes = {{:?}}\",\n\t\t\t\t\t__column,\n\t\t\t\t\t__a,\n\t\t\t\t\t\
		 __b,\n\t\t\t\t\t__ea.as_slice(),\n\t\t\t\t\t__eb.as_slice()\n\t\t\t\t);\n"
	));
	out.push_str(&format!("\t\t\t\tassert_eq!(\n\t\t\t\t\t__actual == Ordering::Equal,\n\t\t\t\t\t__a == \
		 __b,\n\t\t\t\t\t\"{name}: two distinct keys share an encoding, or one key encodes two ways\\n  a \
		 = {{:?}}\\n  b = {{:?}}\\n  a bytes = {{:?}}\\n  b bytes = \
		 {{:?}}\",\n\t\t\t\t\t__a,\n\t\t\t\t\t__b,\n\t\t\t\t\t__ea.as_slice(),\n\t\t\t\t\t\
		 __eb.as_slice()\n\t\t\t\t);\n"));
	out.push_str(&format!("\t\t\t\tassert_eq!(\n\t\t\t\t\t__a.fields().cmp(&__b.fields()),\n\t\t\t\t\t\
		 __actual,\n\t\t\t\t\t\"{name}: fields() order disagrees with encoded byte order\\n  a = \
		 {{:?}}\\n  b = {{:?}}\\n  a bytes = {{:?}}\\n  b bytes = \
		 {{:?}}\",\n\t\t\t\t\t__a,\n\t\t\t\t\t__b,\n\t\t\t\t\t__ea.as_slice(),\n\t\t\t\t\t\
		 __eb.as_slice()\n\t\t\t\t);\n"));
	out.push_str("\t\t\t}\n\t\t}\n\t}\n");
	out
}
