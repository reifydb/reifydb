// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::Error;
use serde::{
	Serialize,
	ser::{Impossible, SerializeSeq, SerializeTuple, SerializeTupleVariant},
};

use super::{
	encode_bool, encode_bytes, encode_f32, encode_f64, encode_i8, encode_i16, encode_i32, encode_i64, encode_i128,
	encode_u8, encode_u16, encode_u32, encode_u64, encode_u128,
};

pub(crate) struct Serializer {
	pub(crate) output: Vec<u8>,
}

impl serde::ser::Serializer for &mut Serializer {
	type Ok = ();
	type Error = Error;

	type SerializeSeq = Self;
	type SerializeTuple = Self;
	type SerializeTupleStruct = Impossible<(), Error>;
	type SerializeTupleVariant = Self;
	type SerializeMap = Impossible<(), Error>;
	type SerializeStruct = Impossible<(), Error>;
	type SerializeStructVariant = Impossible<(), Error>;

	fn serialize_bool(self, v: bool) -> reifydb_type::Result<()> {
		self.output.push(encode_bool(v));
		Ok(())
	}

	fn serialize_i8(self, v: i8) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_i8(v));
		Ok(())
	}

	fn serialize_i16(self, v: i16) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_i16(v));
		Ok(())
	}

	fn serialize_i32(self, v: i32) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_i32(v));
		Ok(())
	}

	fn serialize_i64(self, v: i64) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_i64(v));
		Ok(())
	}

	fn serialize_i128(self, v: i128) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_i128(v));
		Ok(())
	}

	fn serialize_u8(self, v: u8) -> reifydb_type::Result<()> {
		self.output.push(encode_u8(v));
		Ok(())
	}

	fn serialize_u16(self, v: u16) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_u16(v));
		Ok(())
	}

	fn serialize_u32(self, v: u32) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_u32(v));
		Ok(())
	}

	fn serialize_u64(self, v: u64) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_u64(v));
		Ok(())
	}

	fn serialize_u128(self, v: u128) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_u128(v));
		Ok(())
	}

	fn serialize_f32(self, v: f32) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_f32(v));
		Ok(())
	}

	fn serialize_f64(self, v: f64) -> reifydb_type::Result<()> {
		self.output.extend_from_slice(&encode_f64(v));
		Ok(())
	}

	fn serialize_char(self, _: char) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_str(self, v: &str) -> reifydb_type::Result<()> {
		self.serialize_bytes(v.as_bytes())
	}

	fn serialize_bytes(self, v: &[u8]) -> reifydb_type::Result<()> {
		encode_bytes(v, &mut self.output);
		Ok(())
	}

	fn serialize_none(self) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_unit(self) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_unit_struct(self, _: &'static str) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_unit_variant(self, _: &'static str, index: u32, _: &'static str) -> reifydb_type::Result<()> {
		self.output.push(index.try_into()?);
		Ok(())
	}

	fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> reifydb_type::Result<()> {
		unimplemented!()
	}

	fn serialize_newtype_variant<T: Serialize + ?Sized>(
		self,
		name: &'static str,
		index: u32,
		variant: &'static str,
		value: &T,
	) -> reifydb_type::Result<()> {
		self.serialize_unit_variant(name, index, variant)?;
		value.serialize(self)
	}

	fn serialize_seq(self, _: Option<usize>) -> reifydb_type::Result<Self::SerializeSeq> {
		Ok(self)
	}

	fn serialize_tuple(self, _: usize) -> reifydb_type::Result<Self::SerializeTuple> {
		Ok(self)
	}

	fn serialize_tuple_struct(self, _: &'static str, _: usize) -> reifydb_type::Result<Self::SerializeTupleStruct> {
		unimplemented!()
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		index: u32,
		variant: &'static str,
		_: usize,
	) -> reifydb_type::Result<Self::SerializeTupleVariant> {
		self.serialize_unit_variant(name, index, variant)?;
		Ok(self)
	}

	fn serialize_map(self, _: Option<usize>) -> reifydb_type::Result<Self::SerializeMap> {
		unimplemented!()
	}

	fn serialize_struct(self, _: &'static str, _: usize) -> reifydb_type::Result<Self::SerializeStruct> {
		unimplemented!()
	}

	fn serialize_struct_variant(
		self,
		_: &'static str,
		_: u32,
		_: &'static str,
		_: usize,
	) -> reifydb_type::Result<Self::SerializeStructVariant> {
		unimplemented!()
	}
}

impl SerializeSeq for &mut Serializer {
	type Ok = ();
	type Error = Error;

	fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> reifydb_type::Result<()> {
		value.serialize(&mut **self)
	}

	fn end(self) -> reifydb_type::Result<()> {
		Ok(())
	}
}

impl SerializeTuple for &mut Serializer {
	type Ok = ();
	type Error = Error;

	fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> reifydb_type::Result<()> {
		value.serialize(&mut **self)
	}

	fn end(self) -> reifydb_type::Result<()> {
		Ok(())
	}
}

impl SerializeTupleVariant for &mut Serializer {
	type Ok = ();
	type Error = Error;

	fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> reifydb_type::Result<()> {
		value.serialize(&mut **self)
	}

	fn end(self) -> reifydb_type::Result<()> {
		Ok(())
	}
}
