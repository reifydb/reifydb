// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

//! Bincode is used to encode values, both in key-value stores and the network
//! protocol It is a Rust-specific encoding that depends on the
//! internal data structures being stable, but that is sufficient for now. See:
//! <https://github.com/bincode-org/bincode>
//!
//! This module wraps the [`bincode`] crate and uses the standard config.

use std::io::{Read, Write};

use crate::error::diagnostic::serialization;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Use the standard Bincode configuration.
const CONFIG: bincode::config::Configuration = bincode::config::standard();

/// Serializes a value using Bincode.
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
	// Panic on failure, as this is a problem with the data structure.
	bincode::serde::encode_to_vec(value, CONFIG)
		.expect("value must be serializable")
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: Deserialize<'de>>(
	bytes: &'de [u8],
) -> crate::Result<T> {
	Ok(bincode::serde::borrow_decode_from_slice(bytes, CONFIG)?.0)
}

/// Serializes a value to a writer using Bincode.
pub fn serialize_into<W: Write, T: Serialize>(
	mut writer: W,
	value: &T,
) -> crate::Result<()> {
	bincode::serde::encode_into_std_write(value, &mut writer, CONFIG)?;
	Ok(())
}

/// Deserializes a value from a reader using Bincode.
pub fn deserialize_from<R: Read, T: DeserializeOwned>(
	mut reader: R,
) -> crate::Result<T> {
	Ok(bincode::serde::decode_from_std_read(&mut reader, CONFIG)?)
}

/// Deserializes a value from a reader using Bincode, or returns None if the
/// reader is closed.
pub fn maybe_deserialize_from<R: Read, T: DeserializeOwned>(
	mut reader: R,
) -> crate::Result<Option<T>> {
	match bincode::serde::decode_from_std_read(&mut reader, CONFIG) {
		Ok(t) => Ok(Some(t)),
		Err(bincode::error::DecodeError::Io {
			inner,
			..
		}) if inner.kind() == std::io::ErrorKind::UnexpectedEof
			|| inner.kind()
				== std::io::ErrorKind::ConnectionReset =>
		{
			Ok(None)
		}
		Err(err) => Err(crate::error!(
			serialization::bincode_decode_error(err)
		)),
	}
}

#[cfg(test)]
mod tests {
	// use crate::util::encoding::{Value as _, bincode};
	// use crate::{Row, Value};
	//
	// #[test]
	// fn test() {
	//     let encoded = bincode::serialize(&vec![vec![Value::Int1(1)]]);
	//     let r = Row::decode(&encoded);
	//     dbg!(&r);
	// }
}
