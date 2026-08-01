// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::to_extend;
use serde::{Deserialize, Serialize};

use super::Value;
use crate::util::hash::xxh3_128;

#[repr(transparent)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Partition(pub u128);

impl Partition {
	/// Seedless `xxh3_128` over the concatenated postcard encodings, so the partition of a row is
	/// identical on every node and across restarts. Values are appended without a separator, which
	/// only stays unambiguous because postcard is self-delimiting.
	pub fn of(values: &[Value]) -> Self {
		let mut buf: Vec<u8> = Vec::new();
		for value in values {
			buf = to_extend(value, buf).expect("postcard serialization of a Value is total");
		}
		Partition(xxh3_128(&buf).0)
	}
}

impl From<u128> for Partition {
	fn from(value: u128) -> Self {
		Partition(value)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_deterministic() {
		let a = Partition::of(&[Value::Utf8("us".to_string())]);
		let b = Partition::of(&[Value::Utf8("us".to_string())]);
		assert_eq!(a, b, "same values must hash to the same partition");
	}

	#[test]
	fn test_distinct_values_distinct_partition() {
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
		assert_ne!(us, eu, "different values should (overwhelmingly) hash differently");
	}

	#[test]
	fn test_multi_column() {
		let a = Partition::of(&[Value::Utf8("us".to_string()), Value::Uint8(1)]);
		let b = Partition::of(&[Value::Utf8("us".to_string()), Value::Uint8(2)]);
		assert_ne!(a, b);
	}
}
