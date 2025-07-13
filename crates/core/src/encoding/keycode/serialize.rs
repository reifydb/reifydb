// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::encoding;
use crate::encoding::Error;
use serde::Serialize;
use serde::ser::{Impossible, SerializeSeq, SerializeTuple, SerializeTupleVariant};

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

    fn serialize_bool(self, v: bool) -> encoding::Result<()> {
        self.output.push(if v { 0x00 } else { 0x01 });
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7; // flip sign bit
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> encoding::Result<()> {
        self.output.push(!v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(&bytes);
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            false => bytes[0] ^= 1 << 7, // positive, flip sign bit
            true => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
        }
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> encoding::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            false => bytes[0] ^= 1 << 7, // positive, flip sign bit
            true => bytes.iter_mut().for_each(|b| *b = !*b), // negative, flip all bits
        }
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_char(self, _: char) -> encoding::Result<()> {
        unimplemented!()
    }

    fn serialize_str(self, v: &str) -> encoding::Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> encoding::Result<()> {
        for &byte in v {
            if byte == 0xff {
                self.output.push(0xff);
                self.output.push(0x00);
            } else {
                self.output.push(byte);
            }
        }
        self.output.push(0xff);
        self.output.push(0xff);

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

    fn serialize_seq(self, _: Option<usize>) -> encoding::Result<Self::SerializeSeq> {
        Ok(self)
    }

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
