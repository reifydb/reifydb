// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::SeriesId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesKey {
	pub series: SeriesId,
}

impl SeriesKey {
	pub fn new(series: SeriesId) -> Self {
		Self {
			series,
		}
	}

	pub fn encoded(series: impl Into<SeriesId>) -> EncodedKey {
		Self::new(series.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::series_start()), Some(Self::series_end()))
	}

	fn series_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn series_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for SeriesKey {
	const KIND: KeyKind = KeyKind::Series;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.series);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let series = de.read_u64().ok()?;

		Some(Self {
			series: SeriesId(series),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesMetadataKey {
	pub series: SeriesId,
}

impl SeriesMetadataKey {
	pub fn new(series: SeriesId) -> Self {
		Self {
			series,
		}
	}

	pub fn encoded(series: impl Into<SeriesId>) -> EncodedKey {
		Self::new(series.into()).encode()
	}
}

impl EncodableKey for SeriesMetadataKey {
	const KIND: KeyKind = KeyKind::SeriesMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.series);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let series = de.read_u64().ok()?;

		Some(Self {
			series: SeriesId(series),
		})
	}
}
