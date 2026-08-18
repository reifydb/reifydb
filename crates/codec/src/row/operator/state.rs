// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::BTreeMap, mem};

use postcard::{from_bytes, to_extend};
use reifydb_value::{
	error::{Error as ValueError, TypeError},
	value::datetime::DateTime,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::row::pod::EncodedPodRow;

#[derive(Debug, Error, PartialEq)]
pub enum StateError {
	#[error("operator state serialization failed: {0}")]
	Serialization(String),

	#[error("operator state deserialization failed: {0}")]
	Deserialization(String),
}

impl From<StateError> for ValueError {
	fn from(err: StateError) -> Self {
		match err {
			StateError::Serialization(_) => TypeError::SerdeSerialize {
				message: err.to_string(),
			}
			.into(),
			StateError::Deserialization(_) => TypeError::SerdeDeserialize {
				message: err.to_string(),
			}
			.into(),
		}
	}
}

pub trait OperatorState: Sized + Send + 'static {
	fn encode_state(&self) -> Result<EncodedPodRow, StateError>;

	fn decode_state(row: &EncodedPodRow) -> Result<Self, StateError>;
}

thread_local! {
	static ENCODE_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn encode<T>(value: &T) -> Result<EncodedPodRow, StateError>
where
	T: Serialize,
{
	let mut buffer = ENCODE_BUFFER.with(|cell| mem::take(&mut *cell.borrow_mut()));
	buffer.clear();
	let mut filled = to_extend(value, buffer).map_err(|e| StateError::Serialization(e.to_string()))?;
	let result = EncodedPodRow::new(&filled);
	filled.clear();
	ENCODE_BUFFER.with(|cell| *cell.borrow_mut() = filled);
	Ok(result)
}

pub fn decode_body<T>(row: &EncodedPodRow) -> Result<T, StateError>
where
	T: DeserializeOwned,
{
	from_bytes(row.body()).map_err(|e| StateError::Deserialization(e.to_string()))
}

pub fn decode<T: OperatorState>(row: &EncodedPodRow) -> Result<T, StateError> {
	T::decode_state(row)
}

pub mod derive {
	pub use serde::{self, Deserialize, Serialize};
}

pub trait StateCodec: Sized + Send + 'static + Serialize + DeserializeOwned {}

impl<T> StateCodec for T where T: Sized + Send + 'static + Serialize + DeserializeOwned {}

macro_rules! leaf_operator_state {
	($($ty:ty),* $(,)?) => {
		$(impl OperatorState for $ty {
			fn encode_state(&self) -> Result<EncodedPodRow, StateError> {
				encode(self)
			}

			fn decode_state(row: &EncodedPodRow) -> Result<Self, StateError> {
				decode_body::<Self>(row)
			}
		})*
	};
}

leaf_operator_state!(u64, i64, Vec<u8>, (i64, i64, i64), DateTime);

impl<K, V> OperatorState for BTreeMap<K, V>
where
	K: Send + 'static,
	V: Send + 'static,
	Self: Serialize + DeserializeOwned,
{
	fn encode_state(&self) -> Result<EncodedPodRow, StateError> {
		encode(self)
	}

	fn decode_state(row: &EncodedPodRow) -> Result<Self, StateError> {
		decode_body::<Self>(row)
	}
}
