// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

use crate::generate::compile_error;

struct FlatField {
	name: String,
	column: FlatColumn,
	ty: String,
}

#[derive(Clone, Copy)]
enum FlatColumn {
	U8,
	U16,
	U32,
	U64,
	U128,
	RowNumber,
	GroupId,
	Blob16,
	TableId,
	ColumnId,
	ObjectId,
	StorageId,
	FlowId,
	FlowEdgeId,
	OperatorId,
	HandlerId,
	NamespaceId,
	SumTypeId,
	SequenceId,
	EpochSeconds,
	IdentityId,
	ViewId,
	SeriesId,
	SinkId,
	SourceId,
	QueueId,
	RingBufferId,
	BindingId,
	DictionaryId,
	ProcedureId,
	DateTime,
	Partition,
	ColumnPropertyId,
	RowShapeFingerprint,
	RelationshipId,
	MigrationId,
	MigrationEventId,
	PrimaryKeyId,
	IndexId,
	SystemVersion,
	OptionU8,
}

impl FlatColumn {
	fn width(self) -> usize {
		match self {
			FlatColumn::U8 => 1,
			FlatColumn::U16 => 2,
			FlatColumn::U32 => 4,
			FlatColumn::U64
			| FlatColumn::RowNumber
			| FlatColumn::TableId
			| FlatColumn::ColumnId
			| FlatColumn::FlowId
			| FlatColumn::FlowEdgeId
			| FlatColumn::OperatorId
			| FlatColumn::HandlerId
			| FlatColumn::NamespaceId
			| FlatColumn::SumTypeId
			| FlatColumn::SequenceId
			| FlatColumn::EpochSeconds
			| FlatColumn::ViewId
			| FlatColumn::SeriesId
			| FlatColumn::SinkId
			| FlatColumn::SourceId
			| FlatColumn::QueueId
			| FlatColumn::RingBufferId
			| FlatColumn::BindingId
			| FlatColumn::DictionaryId
			| FlatColumn::ProcedureId
			| FlatColumn::DateTime
			| FlatColumn::ColumnPropertyId
			| FlatColumn::RowShapeFingerprint
			| FlatColumn::RelationshipId
			| FlatColumn::MigrationId
			| FlatColumn::MigrationEventId
			| FlatColumn::PrimaryKeyId
			| FlatColumn::IndexId => 8,
			FlatColumn::ObjectId | FlatColumn::StorageId => 9,
			FlatColumn::SystemVersion => 1,
			FlatColumn::OptionU8 => 2,
			FlatColumn::U128
			| FlatColumn::GroupId
			| FlatColumn::Blob16
			| FlatColumn::Partition
			| FlatColumn::IdentityId => 16,
		}
	}

	fn encode_stmt(self, field: &str) -> String {
		match self {
			FlatColumn::U8 => format!("serializer.extend_u8(self.{field});"),
			FlatColumn::U16 => format!("serializer.extend_u16(self.{field});"),
			FlatColumn::U32 => format!("serializer.extend_u32(self.{field});"),
			FlatColumn::U64 => format!("serializer.extend_u64(self.{field});"),
			FlatColumn::U128 => format!("serializer.extend_u128(self.{field});"),
			FlatColumn::RowNumber => format!("serializer.extend_u64(self.{field}.0);"),
			FlatColumn::GroupId => format!("serializer.extend_u128(self.{field}.0);"),
			FlatColumn::Blob16 => format!("serializer.extend_raw(&self.{field});"),
			FlatColumn::IdentityId => format!("serializer.extend_identity_id(&self.{field});"),
			FlatColumn::TableId
			| FlatColumn::ColumnId
			| FlatColumn::FlowId
			| FlatColumn::FlowEdgeId
			| FlatColumn::OperatorId
			| FlatColumn::HandlerId
			| FlatColumn::NamespaceId
			| FlatColumn::SumTypeId
			| FlatColumn::SequenceId
			| FlatColumn::ViewId
			| FlatColumn::SeriesId
			| FlatColumn::SinkId
			| FlatColumn::SourceId
			| FlatColumn::QueueId
			| FlatColumn::RingBufferId
			| FlatColumn::BindingId
			| FlatColumn::DictionaryId
			| FlatColumn::ColumnPropertyId
			| FlatColumn::RelationshipId
			| FlatColumn::MigrationId
			| FlatColumn::MigrationEventId
			| FlatColumn::PrimaryKeyId => format!("serializer.extend_u64(self.{field}.0);"),
			FlatColumn::ProcedureId => format!("serializer.extend_u64(*self.{field});"),
			FlatColumn::EpochSeconds => format!("serializer.extend_u64(self.{field}.seconds());"),
			FlatColumn::DateTime => format!("serializer.extend_datetime(&self.{field});"),
			FlatColumn::RowShapeFingerprint => format!("serializer.extend_u64(self.{field}.as_u64());"),
			FlatColumn::Partition => format!("serializer.extend_u128(self.{field}.0);"),
			FlatColumn::IndexId => format!("serializer.extend_u64(self.{field}.as_u64());"),
			FlatColumn::SystemVersion => format!("serializer.extend_u8(self.{field} as u8);"),
			FlatColumn::ObjectId | FlatColumn::StorageId => {
				format!("serializer.extend_object_id(self.{field});")
			}
			FlatColumn::OptionU8 => format!(
				"match self.{field} {{ Some(v) => {{ serializer.extend_u8(1u8).extend_u8(v); }} \
				 None => {{ serializer.extend_u8(0u8).extend_u8(0u8); }} }}"
			),
		}
	}

	fn decode_expr(self, field_type: &str) -> String {
		match self {
			FlatColumn::U8 => "de.read_u8().ok()?".to_string(),
			FlatColumn::U16 => "de.read_u16().ok()?".to_string(),
			FlatColumn::U32 => "de.read_u32().ok()?".to_string(),
			FlatColumn::U64 => "de.read_u64().ok()?".to_string(),
			FlatColumn::U128 => "de.read_u128().ok()?".to_string(),
			FlatColumn::RowNumber => "RowNumber(de.read_u64().ok()?)".to_string(),
			FlatColumn::GroupId => "GroupId(de.read_u128().ok()?)".to_string(),
			FlatColumn::Blob16 => "{ let bytes = de.read_raw(16).ok()?; let mut buf = [0u8; 16]; \
				 buf.copy_from_slice(bytes); buf }"
				.to_string(),
			FlatColumn::IdentityId => "de.read_identity_id().ok()?".to_string(),
			FlatColumn::TableId
			| FlatColumn::ColumnId
			| FlatColumn::FlowId
			| FlatColumn::FlowEdgeId
			| FlatColumn::OperatorId
			| FlatColumn::HandlerId
			| FlatColumn::NamespaceId
			| FlatColumn::SumTypeId
			| FlatColumn::SequenceId
			| FlatColumn::ViewId
			| FlatColumn::SeriesId
			| FlatColumn::SinkId
			| FlatColumn::SourceId
			| FlatColumn::QueueId
			| FlatColumn::RingBufferId
			| FlatColumn::BindingId
			| FlatColumn::DictionaryId
			| FlatColumn::ColumnPropertyId
			| FlatColumn::RelationshipId
			| FlatColumn::MigrationId
			| FlatColumn::MigrationEventId
			| FlatColumn::PrimaryKeyId => format!("{field_type}(de.read_u64().ok()?)"),
			FlatColumn::ProcedureId => "ProcedureId::from_raw(de.read_u64().ok()?)".to_string(),
			FlatColumn::EpochSeconds => "EpochSeconds::new(de.read_u64().ok()?)".to_string(),
			FlatColumn::DateTime => "de.read_datetime().ok()?".to_string(),
			FlatColumn::RowShapeFingerprint => "RowShapeFingerprint::new(de.read_u64().ok()?)".to_string(),
			FlatColumn::Partition => "Partition(de.read_u128().ok()?)".to_string(),
			FlatColumn::IndexId => "IndexId::Primary(PrimaryKeyId(de.read_u64().ok()?))".to_string(),
			FlatColumn::SystemVersion => format!("{field_type}::try_from(de.read_u8().ok()?).ok()?"),
			FlatColumn::ObjectId => "de.read_object_id().ok()?".to_string(),
			FlatColumn::StorageId => "StorageId::from_object(de.read_object_id().ok()?)?".to_string(),
			FlatColumn::OptionU8 => "match de.read_u8().ok()? { 1u8 => Some(de.read_u8().ok()?), 0u8 => { \
				de.read_u8().ok()?; None }, _ => return None }"
				.to_string(),
		}
	}
}

pub fn derive_key(input: TokenStream) -> TokenStream {
	let tokens: Vec<TokenTree> = input.into_iter().collect();
	let mut iter = tokens.iter().peekable();

	let mut kind: Option<String> = None;
	while let Some(TokenTree::Punct(p)) = iter.peek() {
		if p.as_char() != '#' {
			break;
		}
		iter.next();
		match iter.next() {
			Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Bracket => {
				if let Some(found) = parse_key_attribute(g) {
					kind = Some(found);
				}
			}
			_ => return compile_error("expected an attribute after '#'"),
		}
	}

	let kind = match kind {
		Some(kind) => kind,
		None => return compile_error("Key requires #[key(kind = Variant)] naming its KeyKind"),
	};

	if let Some(TokenTree::Ident(i)) = iter.peek()
		&& *i == "pub"
	{
		iter.next();
		if let Some(TokenTree::Group(g)) = iter.peek()
			&& g.delimiter() == Delimiter::Parenthesis
		{
			iter.next();
		}
	}

	match iter.next() {
		Some(TokenTree::Ident(i)) if *i == "struct" => {}
		_ => return compile_error("Key can only be derived for structs"),
	}

	let name = match iter.next() {
		Some(TokenTree::Ident(i)) => i.to_string(),
		_ => return compile_error("expected struct name"),
	};

	let body = match iter.next() {
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Brace => g.clone(),
		Some(TokenTree::Punct(p)) if p.as_char() == '<' => {
			return compile_error("Key cannot be derived for a generic struct");
		}
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => {
			return compile_error("Key requires named fields, so a tuple struct has no field order");
		}
		_ => return compile_error("expected struct body"),
	};

	let fields = match parse_fields(&body) {
		Ok(fields) => fields,
		Err(err) => return err,
	};

	expand(&name, &kind, &fields)
}

fn parse_key_attribute(group: &Group) -> Option<String> {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	let mut iter = tokens.iter();

	match iter.next() {
		Some(TokenTree::Ident(i)) if *i == "key" => {}
		_ => return None,
	}

	let inner = match iter.next() {
		Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g.clone(),
		_ => return None,
	};

	let inner_tokens: Vec<TokenTree> = inner.stream().into_iter().collect();
	match inner_tokens.as_slice() {
		[TokenTree::Ident(kind_ident), TokenTree::Punct(eq), TokenTree::Ident(variant)]
			if *kind_ident == "kind" && eq.as_char() == '=' =>
		{
			Some(variant.to_string())
		}
		_ => None,
	}
}

fn parse_fields(body: &Group) -> Result<Vec<FlatField>, TokenStream> {
	let tokens: Vec<TokenTree> = body.stream().into_iter().collect();
	let mut iter = tokens.iter().peekable();
	let mut fields = Vec::new();

	while iter.peek().is_some() {
		while let Some(TokenTree::Punct(p)) = iter.peek() {
			if p.as_char() == '#' {
				iter.next();
				if let Some(TokenTree::Group(_)) = iter.peek() {
					iter.next();
				}
			} else {
				break;
			}
		}
		if iter.peek().is_none() {
			break;
		}

		if let Some(TokenTree::Ident(i)) = iter.peek()
			&& *i == "pub"
		{
			iter.next();
			if let Some(TokenTree::Group(g)) = iter.peek()
				&& g.delimiter() == Delimiter::Parenthesis
			{
				iter.next();
			}
		}

		let field_name = match iter.next() {
			Some(TokenTree::Ident(i)) => i.to_string(),
			_ => return Err(compile_error("expected field name")),
		};

		match iter.next() {
			Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
			_ => return Err(compile_error("expected ':' after field name")),
		}

		let mut ty_tokens: Vec<TokenTree> = Vec::new();
		loop {
			match iter.peek() {
				Some(TokenTree::Punct(p)) if p.as_char() == ',' => {
					iter.next();
					break;
				}
				Some(t) => {
					ty_tokens.push((*t).clone());
					iter.next();
				}
				None => break,
			}
		}

		if ty_tokens.is_empty() {
			return Err(compile_error(&format!("field '{}' has no type", field_name)));
		}

		let ty = render(&ty_tokens);
		let column = match column_type(&ty_tokens) {
			Some(column) => column,
			None => {
				return Err(compile_error(&format!(
					"field '{}' has type '{}', which has no flat key column type",
					field_name, ty
				)));
			}
		};

		fields.push(FlatField {
			name: field_name,
			column,
			ty,
		});
	}

	Ok(fields)
}

fn column_type(tokens: &[TokenTree]) -> Option<FlatColumn> {
	if let [TokenTree::Group(group)] = tokens {
		return (group.delimiter() == Delimiter::Bracket && is_byte_array_16(group))
			.then_some(FlatColumn::Blob16);
	}

	if is_option_u8(tokens) {
		return Some(FlatColumn::OptionU8);
	}

	let unqualified = strip_path(tokens);
	if unqualified.len() != 1 {
		return None;
	}

	let head = match unqualified.first() {
		Some(TokenTree::Ident(i)) => i.to_string(),
		_ => return None,
	};

	match head.as_str() {
		"u8" => Some(FlatColumn::U8),
		"u16" => Some(FlatColumn::U16),
		"u32" => Some(FlatColumn::U32),
		"u64" => Some(FlatColumn::U64),
		"u128" => Some(FlatColumn::U128),
		"RowNumber" => Some(FlatColumn::RowNumber),
		"GroupId" => Some(FlatColumn::GroupId),
		"TableId" => Some(FlatColumn::TableId),
		"ColumnId" => Some(FlatColumn::ColumnId),
		"ObjectId" => Some(FlatColumn::ObjectId),
		"StorageId" => Some(FlatColumn::StorageId),
		"QueueId" => Some(FlatColumn::QueueId),
		"RingBufferId" => Some(FlatColumn::RingBufferId),
		"SeriesId" => Some(FlatColumn::SeriesId),
		"DateTime" => Some(FlatColumn::DateTime),
		"Partition" => Some(FlatColumn::Partition),
		"FlowId" => Some(FlatColumn::FlowId),
		"FlowEdgeId" => Some(FlatColumn::FlowEdgeId),
		"OperatorId" => Some(FlatColumn::OperatorId),
		"HandlerId" => Some(FlatColumn::HandlerId),
		"NamespaceId" => Some(FlatColumn::NamespaceId),
		"SumTypeId" => Some(FlatColumn::SumTypeId),
		"SequenceId" => Some(FlatColumn::SequenceId),
		"EpochSeconds" => Some(FlatColumn::EpochSeconds),
		"IdentityId" => Some(FlatColumn::IdentityId),
		"AuthenticationId" | "IdentityAttributeId" | "RoleId" | "PolicyId" | "TokenId" => Some(FlatColumn::U64),
		"ViewId" => Some(FlatColumn::ViewId),
		"SinkId" => Some(FlatColumn::SinkId),
		"SourceId" => Some(FlatColumn::SourceId),
		"BindingId" => Some(FlatColumn::BindingId),
		"DictionaryId" => Some(FlatColumn::DictionaryId),
		"ProcedureId" => Some(FlatColumn::ProcedureId),
		"ColumnPropertyId" => Some(FlatColumn::ColumnPropertyId),
		"RowShapeFingerprint" => Some(FlatColumn::RowShapeFingerprint),
		"IndexId" => Some(FlatColumn::IndexId),
		_ => None,
	}
}

fn is_option_u8(tokens: &[TokenTree]) -> bool {
	matches!(
		tokens,
		[TokenTree::Ident(o), TokenTree::Punct(lt), TokenTree::Ident(u), TokenTree::Punct(gt)]
			if *o == "Option" && lt.as_char() == '<' && *u == "u8" && gt.as_char() == '>'
	)
}

fn is_byte_array_16(group: &Group) -> bool {
	let tokens: Vec<TokenTree> = group.stream().into_iter().collect();
	match tokens.as_slice() {
		[TokenTree::Ident(element), TokenTree::Punct(semi), TokenTree::Literal(len)] => {
			*element == "u8" && semi.as_char() == ';' && len.to_string() == "16"
		}
		_ => false,
	}
}

fn strip_path(tokens: &[TokenTree]) -> &[TokenTree] {
	let mut at = 0;
	if is_colon(tokens.first()) && is_colon(tokens.get(1)) {
		at = 2;
	}
	while matches!(tokens.get(at), Some(TokenTree::Ident(_)))
		&& is_colon(tokens.get(at + 1))
		&& is_colon(tokens.get(at + 2))
	{
		at += 3;
	}
	&tokens[at..]
}

fn is_colon(token: Option<&TokenTree>) -> bool {
	matches!(token, Some(TokenTree::Punct(p)) if p.as_char() == ':')
}

fn render(tokens: &[TokenTree]) -> String {
	tokens.iter().map(|token| token.to_string()).collect()
}

fn expand(name: &str, kind: &str, fields: &[FlatField]) -> TokenStream {
	let capacity: usize = 1 + fields.iter().map(|f| f.column.width()).sum::<usize>();

	let mut encode_body = String::new();
	for field in fields {
		encode_body.push_str("\t\t");
		encode_body.push_str(&field.column.encode_stmt(&field.name));
		encode_body.push('\n');
	}

	let mut decode_body = String::new();
	for field in fields {
		decode_body.push_str(&format!("\t\t\t{}: {},\n", field.name, field.column.decode_expr(&field.ty)));
	}

	let mut out = String::new();
	out.push_str(&format!("#[automatically_derived]\nimpl Key for {name} {{\n"));
	out.push_str(&format!("\tconst KIND: KeyKind = KeyKind::{kind};\n\n"));
	out.push_str("\tfn encode(&self) -> EncodedKey {\n");
	out.push_str(&format!(
		"\t\tlet mut serializer = ::reifydb_codec::key::serializer::KeySerializer::with_capacity({capacity});\n"
	));
	out.push_str("\t\tserializer.extend_u8(<Self as Key>::KIND as u8);\n");
	out.push_str(&encode_body);
	out.push_str("\t\tserializer.to_encoded_key()\n\t}\n\n");
	out.push_str("\tfn decode(key: &EncodedKey) -> Option<Self> {\n");
	out.push_str(
		"\t\tlet mut de = ::reifydb_codec::key::deserializer::KeyDeserializer::from_bytes(key.as_slice());\n",
	);
	out.push_str("\t\tlet found: KeyKind = de.read_u8().ok()?.try_into().ok()?;\n");
	out.push_str("\t\tif found != <Self as Key>::KIND {\n\t\t\treturn None;\n\t\t}\n");
	out.push_str(&format!("\t\tSome(Self {{\n{decode_body}\t\t}})\n\t}}\n}}"));

	out.parse().expect("derived Key impl must be valid Rust")
}

#[cfg(test)]
mod tests {
	use super::derive_key;

	fn expand(source: &str) -> String {
		derive_key(source.parse().unwrap()).to_string()
	}

	#[test]
	fn missing_attribute_is_rejected() {
		let out = expand("struct RowKey { row: u64 }");
		assert!(out.contains("compile_error"), "{out}");
		assert!(out.contains("key"), "{out}");
	}

	#[test]
	fn a_well_formed_struct_expands_without_an_error() {
		let out = expand("#[key(kind = Row)] struct RowKey { table: u64, row: RowNumber }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("impl Key for RowKey"), "{out}");
		assert!(out.contains("KeyKind :: Row"), "{out}");
	}

	#[test]
	fn fields_are_encoded_in_declaration_order() {
		let out = expand("#[key(kind = Row)] struct RowKey { table: u64, row: RowNumber }");
		let table = out.find("self . table").expect("table field encoded");
		let row = out.find("self . row").expect("row field encoded");
		assert!(table < row, "{out}");
	}

	#[test]
	fn an_unrecognised_field_type_is_rejected() {
		let out = expand("#[key(kind = Row)] struct RowKey { at: String }");
		assert!(out.contains("compile_error"), "{out}");
		assert!(out.contains("at"), "{out}");
		assert!(out.contains("String"), "{out}");
	}

	#[test]
	fn a_tuple_struct_is_rejected() {
		let out = expand("#[key(kind = Row)] struct RowKey(u64);");
		assert!(out.contains("compile_error"), "{out}");
	}

	#[test]
	fn a_generic_struct_is_rejected() {
		let out = expand("#[key(kind = Row)] struct RowKey<T> { row: T }");
		assert!(out.contains("compile_error"), "{out}");
	}

	#[test]
	fn a_byte_array_field_maps_to_blob16() {
		let out = expand("#[key(kind = Row)] struct RowKey { blob: [u8; 16] }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("extend_raw"));
	}

	#[test]
	fn a_byte_array_of_the_wrong_width_is_rejected() {
		let out = expand("#[key(kind = Row)] struct RowKey { blob: [u8; 32] }");
		assert!(out.contains("compile_error"), "{out}");
	}

	#[test]
	fn table_id_and_column_id_encode_via_their_wrapped_u64() {
		let out = expand("#[key(kind = Table)] struct TableKey { table: TableId, column: ColumnId }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("self . table . 0"), "{out}");
		assert!(out.contains("TableId (de . read_u64 () . ok () ?)"), "{out}");
		assert!(out.contains("ColumnId (de . read_u64 () . ok () ?)"), "{out}");
	}

	#[test]
	fn an_option_u8_field_encodes_as_a_fixed_two_byte_presence_and_value_pair() {
		let out = expand("#[key(kind = Row)] struct RowKey { tag: Option<u8> }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("with_capacity (3)"), "{out}: width must stay fixed at 2 bytes for None and Some");
		assert!(out.contains("Some (v) => { serializer . extend_u8 (1u8) . extend_u8 (v) ; }"), "{out}");
		assert!(out.contains("None => { serializer . extend_u8 (0u8) . extend_u8 (0u8) ; }"), "{out}");
		assert!(out.contains("1u8 => Some (de . read_u8 () . ok () ?)"), "{out}");
		assert!(out.contains("0u8 => { de . read_u8 () . ok () ?; None }"), "{out}");
	}

	#[test]
	fn object_id_and_storage_id_encode_via_the_catalog_ext() {
		let out = expand("#[key(kind = Row)] struct RowKey { storage: StorageId, object: ObjectId }");
		assert!(!out.contains("compile_error"), "{out}");
		assert!(out.contains("extend_object_id (self . storage)"), "{out}");
		assert!(out.contains("extend_object_id (self . object)"), "{out}");
		assert!(out.contains("StorageId :: from_object (de . read_object_id () . ok () ?) ?"), "{out}");
		assert!(out.contains("de . read_object_id () . ok () ?"), "{out}");
	}
}
