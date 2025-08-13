// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::de::{
	DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess,
	VariantAccess, Visitor,
};

use crate::util::encoding::Error;

pub(crate) struct Deserializer<'de> {
	pub(crate) input: &'de [u8],
}

impl<'de> Deserializer<'de> {
	pub fn from_bytes(input: &'de [u8]) -> Self {
		Deserializer {
			input,
		}
	}

	fn take_bytes(&mut self, len: usize) -> crate::Result<&[u8]> {
		if self.input.len() < len {
			return Err(crate::error!(
                crate::error::diagnostic::serialization::keycode_serialization_error(format!(
                    "insufficient bytes, expected {len} bytes for {:x?}",
                    self.input
                ))
            ));
		}
		let bytes = &self.input[..len];
		self.input = &self.input[len..];
		Ok(bytes)
	}

	fn decode_next_bytes(&mut self) -> crate::Result<Vec<u8>> {
		let mut decoded = Vec::new();
		let mut iter = self.input.iter().enumerate();
		let taken = loop {
			match iter.next() {
                    Some((_, 0xff)) => match iter.next() {
                        Some((i, 0xff)) => break i + 1,        // terminator
                        Some((_, 0x00)) => decoded.push(0xff), // escaped 0xff
                        _ => return Err(crate::error!(
                            crate::error::diagnostic::serialization::keycode_serialization_error(
                                "invalid escape sequence".to_string()
                            )
                        )),
                    },
                    Some((_, b)) => decoded.push(*b),
                    None => {
                        return Err(crate::error!(
                            crate::error::diagnostic::serialization::keycode_serialization_error(
                                "unexpected end of input".to_string()
                            )
                        ));
                    }
                }
		};
		self.input = &self.input[taken..];
		Ok(decoded)
	}
}

impl<'de> serde::de::Deserializer<'de> for &mut Deserializer<'de> {
	type Error = Error;

	fn deserialize_any<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		panic!("must provide type, Keycode is not self-describing")
	}

	fn deserialize_bool<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		visitor.visit_bool(match self.take_bytes(1)?[0] {
            0x01 => false,
            0x00 => true,
            b => {
                return Err(crate::error!(
                    crate::error::diagnostic::serialization::keycode_serialization_error(format!(
                        "invalid boolean value {b}"
                    ))
                ));
            }
        })
	}

	fn deserialize_i8<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut byte = self.take_bytes(1)?[0];
		byte = !byte;
		byte ^= 1 << 7; // restore original sign
		visitor.visit_i8(byte as i8)
	}

	fn deserialize_i16<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(2)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		bytes[0] ^= 1 << 7;
		visitor.visit_i16(i16::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_i32<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(4)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		bytes[0] ^= 1 << 7;
		visitor.visit_i32(i32::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_i64<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(8)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		bytes[0] ^= 1 << 7;
		visitor.visit_i64(i64::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_i128<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(16)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		bytes[0] ^= 1 << 7;
		visitor.visit_i128(i128::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_u8<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let byte = !self.take_bytes(1)?[0];
		visitor.visit_u8(byte)
	}

	fn deserialize_u16<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(2)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		visitor.visit_u16(u16::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_u32<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(4)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		visitor.visit_u32(u32::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_u64<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(8)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		visitor.visit_u64(u64::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_u128<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(16)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		visitor.visit_u128(u128::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_f32<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(4)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		match bytes[0] >> 7 {
			0 => bytes.iter_mut().for_each(|b| *b = !*b), /* negative, flip all bits */
			1 => bytes[0] ^= 1 << 7,                      /* positive, flip sign bit */
			_ => panic!("bits can only be 0 or 1"),
		}
		visitor.visit_f32(f32::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_f64<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let mut bytes = self.take_bytes(8)?.to_vec();
		for b in &mut bytes {
			*b = !*b;
		}
		match bytes[0] >> 7 {
			0 => bytes.iter_mut().for_each(|b| *b = !*b), /* negative, flip all bits */
			1 => bytes[0] ^= 1 << 7,                      /* positive, flip sign bit */
			_ => panic!("bits can only be 0 or 1"),
		}
		visitor.visit_f64(f64::from_be_bytes(
			bytes.as_slice().try_into()?,
		))
	}

	fn deserialize_char<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_str<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let bytes = self.decode_next_bytes()?;
		visitor.visit_str(&String::from_utf8(bytes)?)
	}

	fn deserialize_string<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let bytes = self.decode_next_bytes()?;
		visitor.visit_string(String::from_utf8(bytes)?)
	}

	fn deserialize_bytes<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let bytes = self.decode_next_bytes()?;
		visitor.visit_bytes(&bytes)
	}

	fn deserialize_byte_buf<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		let bytes = self.decode_next_bytes()?;
		visitor.visit_byte_buf(bytes)
	}

	fn deserialize_option<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_unit<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_unit_struct<V: Visitor<'de>>(
		self,
		_: &'static str,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_newtype_struct<V: Visitor<'de>>(
		self,
		_: &'static str,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_seq<V: Visitor<'de>>(
		self,
		visitor: V,
	) -> crate::Result<V::Value> {
		visitor.visit_seq(self)
	}

	fn deserialize_tuple<V: Visitor<'de>>(
		self,
		_: usize,
		visitor: V,
	) -> crate::Result<V::Value> {
		visitor.visit_seq(self)
	}

	fn deserialize_tuple_struct<V: Visitor<'de>>(
		self,
		_: &'static str,
		_: usize,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_map<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_struct<V: Visitor<'de>>(
		self,
		_: &'static str,
		_: &'static [&'static str],
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_enum<V: Visitor<'de>>(
		self,
		_: &'static str,
		_: &'static [&'static str],
		visitor: V,
	) -> crate::Result<V::Value> {
		visitor.visit_enum(self)
	}

	fn deserialize_identifier<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}

	fn deserialize_ignored_any<V: Visitor<'de>>(
		self,
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}
}

impl<'de> SeqAccess<'de> for Deserializer<'de> {
	type Error = Error;

	fn next_element_seed<T: DeserializeSeed<'de>>(
		&mut self,
		seed: T,
	) -> crate::Result<Option<T::Value>> {
		if self.input.is_empty() {
			return Ok(None);
		}
		seed.deserialize(self).map(Some)
	}
}

impl<'de> EnumAccess<'de> for &mut Deserializer<'de> {
	type Error = Error;
	type Variant = Self;

	fn variant_seed<V: DeserializeSeed<'de>>(
		self,
		seed: V,
	) -> crate::Result<(V::Value, Self::Variant)> {
		let index = self.take_bytes(1)?[0] as u32;
		let value: crate::Result<_> =
			seed.deserialize(index.into_deserializer());
		Ok((value?, self))
	}
}

impl<'de> VariantAccess<'de> for &mut Deserializer<'de> {
	type Error = Error;

	fn unit_variant(self) -> crate::Result<()> {
		Ok(())
	}

	fn newtype_variant_seed<T: DeserializeSeed<'de>>(
		self,
		seed: T,
	) -> crate::Result<T::Value> {
		seed.deserialize(&mut *self)
	}

	fn tuple_variant<V: Visitor<'de>>(
		self,
		_: usize,
		visitor: V,
	) -> crate::Result<V::Value> {
		visitor.visit_seq(self)
	}

	fn struct_variant<V: Visitor<'de>>(
		self,
		_: &'static [&'static str],
		_: V,
	) -> crate::Result<V::Value> {
		unimplemented!()
	}
}
