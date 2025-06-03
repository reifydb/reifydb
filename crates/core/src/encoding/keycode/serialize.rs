// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::encoding;
use crate::encoding::Error;
use serde::ser::{Impossible, SerializeSeq, SerializeTuple, SerializeTupleVariant};
// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
use serde::Serialize;

/// Serializes keys as binary byte vectors.
pub(crate) struct Serializer {
    pub(crate) output: Vec<u8>,
}

impl serde::ser::Serializer for &mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;

    /// bool simply uses 1 for true and 0 for false.
    fn serialize_bool(self, v: bool) -> encoding::Result<()> {
        self.output.push(if v { 1 } else { 0 });
        Ok(())
    }

    /// i8 uses the big-endian two's complement encoding, with the sign bit flipped
    /// to ensure lexicographic ordering matches numeric ordering.
    ///
    /// Flipping the most significant bit maps negative values to start with `0x00`
    /// and positive values to start with `0x80` or higher, making negative numbers
    /// sort before positive ones.
    fn serialize_i8(self, v: i8) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(&bytes);
        Ok(())
    }

    /// i16 uses the big-endian two's complement encoding, with the sign bit flipped
    /// to make the byte representation lexicographically ordered.
    ///
    /// This transformation ensures that comparisons on encoded values produce the
    /// same ordering as signed numeric comparisons.
    fn serialize_i16(self, v: i16) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(&bytes);
        Ok(())
    }

    /// i32 uses the big-endian two's complement encoding, but flips the sign bit
    /// of the most significant byte to enforce correct ordering.
    ///
    /// After the flip, all negative numbers start with `0x00..7F`, and all positive
    /// numbers start with `0x80..FF`, aligning byte-wise comparison with signed
    /// integer ordering.
    fn serialize_i32(self, v: i32) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(&bytes);
        Ok(())
    }

    /// i64 uses the big-endian two's complement encoding, but flips the
    /// left-most sign bit such that negative numbers are ordered before
    /// positive numbers.
    ///
    /// The relative ordering of the remaining bits is already correct: -1, the
    /// largest negative integer, is encoded as 01111111...11111111, ordered
    /// after all other negative integers but before positive integers.
    fn serialize_i64(self, v: i64) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(bytes);
        Ok(())
    }

    /// i128 uses the big-endian two's complement encoding, but flips the
    /// left-most sign bit such that negative numbers are ordered before
    /// positive numbers.
    ///
    /// The relative ordering of the remaining bits is already correct: -1, the
    /// largest negative integer, is encoded as 0111...1111, ordered after all
    /// other negative integers but before positive integers.
    fn serialize_i128(self, v: i128) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> encoding::Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> encoding::Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> encoding::Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    /// u64 simply uses the big-endian encoding.
    fn serialize_u64(self, v: u64) -> encoding::Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> encoding::Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            false => bytes[0] ^= 1 << 7, // positive, flip sign bit
            true => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
        }
        self.output.extend(bytes);
        Ok(())
    }

    /// f64 is encoded in big-endian IEEE 754 form, but it flips the sign bit to
    /// order positive numbers after negative numbers, and also flips all other
    /// bits for negative numbers to order them from smallest to largest. NaN is
    /// ordered at the end.
    fn serialize_f64(self, v: f64) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            false => bytes[0] ^= 1 << 7, // positive, flip sign bit
            true => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_char(self, _: char) -> encoding::Result<()> {
        unimplemented!()
    }

    // Strings are encoded like bytes.
    fn serialize_str(self, v: &str) -> encoding::Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    // Byte slices are terminated by 0x0000, escaping 0x00 as 0x00ff. This
    // ensures that we can detect the end, and that for two overlapping slices,
    // the shorter one orders before the longer one.
    //
    // We can't use e.g. length prefix encoding, since it doesn't sort correctly.
    fn serialize_bytes(self, v: &[u8]) -> encoding::Result<()> {
        for &byte in v {
            if byte == 0x00 {
                self.output.push(0x00);
                self.output.push(0xff);
            } else {
                self.output.push(byte);
            }
        }
        self.output.push(0x00);
        self.output.push(0x00);

        Ok(())
    }

    fn serialize_none(self) -> encoding::Result<()> {
        unimplemented!()
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> encoding::Result<()> {
        unimplemented!()
    }

    fn serialize_unit(self) -> encoding::Result<()> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _: &'static str) -> encoding::Result<()> {
        unimplemented!()
    }

    /// Enum variants are serialized using their index, as a single byte.
    fn serialize_unit_variant(
        self,
        _: &'static str,
        index: u32,
        _: &'static str,
    ) -> encoding::Result<()> {
        self.output.push(index.try_into()?);
        Ok(())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> encoding::Result<()> {
        unimplemented!()
    }

    /// Newtype variants are serialized using the variant index and inner type.
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> encoding::Result<()> {
        self.serialize_unit_variant(name, index, variant)?;
        value.serialize(self)
    }

    /// Sequences are serialized as the concatenation of the serialized elements.
    fn serialize_seq(self, _: Option<usize>) -> encoding::Result<Self::SerializeSeq> {
        Ok(self)
    }

    /// Tuples are serialized as the concatenation of the serialized elements.
    fn serialize_tuple(self, _: usize) -> encoding::Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> encoding::Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    /// Tuple variants are serialized using the variant index and the
    /// concatenation of the serialized elements.
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        _: usize,
    ) -> encoding::Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _: Option<usize>) -> encoding::Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> encoding::Result<Self::SerializeStruct> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> encoding::Result<Self::SerializeStructVariant> {
        unimplemented!()
    }
}

/// Sequences simply concatenate the serialized elements, with no external structure.
impl SerializeSeq for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> encoding::Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> encoding::Result<()> {
        Ok(())
    }
}

/// Tuples, like sequences, simply concatenate the serialized elements.
impl SerializeTuple for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> encoding::Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> encoding::Result<()> {
        Ok(())
    }
}

/// Tuples, like sequences, simply concatenate the serialized elements.
impl SerializeTupleVariant for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> encoding::Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> encoding::Result<()> {
        Ok(())
    }
}
