// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::DefaultHasher,
	fmt::{self, Debug, Formatter},
	hash::{Hash, Hasher},
};

use reifydb_core::encoded::shape::RowShape;
use reifydb_type::value::{Value, row_number::RowNumber};

use super::strategy::RowContent;

pub enum KeyStrategy {
	Sequential,

	HashOf(Vec<String>),

	Custom(Box<dyn Fn(&RowContent) -> RowNumber + Send + Sync>),
}

impl Debug for KeyStrategy {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			KeyStrategy::Sequential => f.debug_tuple("Sequential").finish(),
			KeyStrategy::HashOf(cols) => f.debug_tuple("HashOf").field(cols).finish(),
			KeyStrategy::Custom(_) => f.debug_struct("Custom").finish_non_exhaustive(),
		}
	}
}

impl KeyStrategy {
	pub fn hash_of<I, S>(columns: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		Self::HashOf(columns.into_iter().map(Into::into).collect())
	}

	pub(crate) fn derive(&self, content: &RowContent, next_sequential: u64) -> RowNumber {
		match self {
			KeyStrategy::Sequential => RowNumber(next_sequential),
			KeyStrategy::HashOf(cols) => {
				let mut hasher = DefaultHasher::new();
				for name in cols {
					match content.get(name) {
						Some(v) => hash_value(v, &mut hasher),
						None => hasher.write_u8(0),
					}
				}

				let h = hasher.finish();
				RowNumber(if h == 0 {
					1
				} else {
					h
				})
			}
			KeyStrategy::Custom(f) => f(content),
		}
	}
}

fn hash_value<H: Hasher>(v: &Value, hasher: &mut H) {
	match v {
		Value::Boolean(b) => {
			hasher.write_u8(1);
			b.hash(hasher);
		}
		Value::Int1(x) => {
			hasher.write_u8(2);
			x.hash(hasher);
		}
		Value::Int2(x) => {
			hasher.write_u8(3);
			x.hash(hasher);
		}
		Value::Int4(x) => {
			hasher.write_u8(4);
			x.hash(hasher);
		}
		Value::Int8(x) => {
			hasher.write_u8(5);
			x.hash(hasher);
		}
		Value::Int16(x) => {
			hasher.write_u8(6);
			x.hash(hasher);
		}
		Value::Uint1(x) => {
			hasher.write_u8(7);
			x.hash(hasher);
		}
		Value::Uint2(x) => {
			hasher.write_u8(8);
			x.hash(hasher);
		}
		Value::Uint4(x) => {
			hasher.write_u8(9);
			x.hash(hasher);
		}
		Value::Uint8(x) => {
			hasher.write_u8(10);
			x.hash(hasher);
		}
		Value::Uint16(x) => {
			hasher.write_u8(11);
			x.hash(hasher);
		}
		Value::Utf8(s) => {
			hasher.write_u8(12);
			s.hash(hasher);
		}
		Value::Float4(of) => {
			hasher.write_u8(13);
			of.hash(hasher);
		}
		Value::Float8(of) => {
			hasher.write_u8(14);
			of.hash(hasher);
		}
		Value::None {
			..
		} => {
			hasher.write_u8(0);
		}

		other => {
			hasher.write_u8(255);
			format!("{:?}", other).hash(hasher);
		}
	}
}

pub struct ChaosSchema {
	pub input_shape: RowShape,
	pub output_shape: RowShape,
	pub key_strategy: KeyStrategy,
	pub output_key_columns: Vec<String>,
}

impl ChaosSchema {
	pub(crate) fn validate(&self) -> Result<(), String> {
		for col in &self.output_key_columns {
			if self.output_shape.find_field(col).is_none() {
				return Err(col.clone());
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::encoded::shape::RowShapeField;
	use reifydb_type::value::r#type::Type;

	use super::*;

	fn shape(fields: &[(&str, Type)]) -> RowShape {
		RowShape::new(fields.iter().map(|(n, t)| RowShapeField::unconstrained(*n, t.clone())).collect())
	}

	fn content(values: &[(&str, Value)]) -> RowContent {
		RowContent::from_pairs(values.iter().map(|(n, v)| ((*n).to_string(), v.clone())))
	}

	#[test]
	fn sequential_returns_passed_in_id() {
		let s = KeyStrategy::Sequential;
		let c = content(&[]);
		assert_eq!(s.derive(&c, 42), RowNumber(42));
	}

	#[test]
	fn hash_of_collides_on_matching_keys() {
		let s = KeyStrategy::hash_of(["base", "quote"]);
		let c1 = content(&[("base", Value::utf8("SOL")), ("quote", Value::utf8("USDC"))]);
		let c2 = content(&[("base", Value::utf8("SOL")), ("quote", Value::utf8("USDC"))]);
		// Different non-key columns shouldn't change the hash.
		let c3 = content(&[
			("base", Value::utf8("SOL")),
			("quote", Value::utf8("USDC")),
			("slot", Value::uint8(99u64)),
		]);

		assert_eq!(s.derive(&c1, 0), s.derive(&c2, 0));
		assert_eq!(s.derive(&c1, 0), s.derive(&c3, 0));
	}

	#[test]
	fn hash_of_distinguishes_different_keys() {
		let s = KeyStrategy::hash_of(["base"]);
		let a = content(&[("base", Value::utf8("SOL"))]);
		let b = content(&[("base", Value::utf8("USDC"))]);
		assert_ne!(s.derive(&a, 0), s.derive(&b, 0));
	}

	#[test]
	fn hash_of_never_yields_zero() {
		// Probe a few values; we mostly care that the zero-mapping
		// branch in derive() works.
		let s = KeyStrategy::hash_of(["x"]);
		for i in 0..256u64 {
			let c = content(&[("x", Value::uint8(i))]);
			assert_ne!(s.derive(&c, 0), RowNumber(0));
		}
	}

	#[test]
	fn custom_strategy_passes_through() {
		let s = KeyStrategy::Custom(Box::new(|content| {
			let v = content.u64("slot").unwrap_or(0);
			RowNumber(v * 10)
		}));
		let c = content(&[("slot", Value::uint8(7u64))]);
		assert_eq!(s.derive(&c, 0), RowNumber(70));
	}

	#[test]
	fn schema_validate_catches_missing_output_key_columns() {
		let schema = ChaosSchema {
			input_shape: shape(&[("a", Type::Int8)]),
			output_shape: shape(&[("a", Type::Int8), ("b", Type::Int8)]),
			key_strategy: KeyStrategy::Sequential,
			output_key_columns: vec!["a".into(), "missing".into()],
		};
		let bad = schema.validate().expect_err("should reject typo'd column");
		assert_eq!(bad, "missing", "validate must return the first offending column name verbatim");
	}

	#[test]
	fn schema_validate_accepts_well_formed() {
		let schema = ChaosSchema {
			input_shape: shape(&[("a", Type::Int8)]),
			output_shape: shape(&[("a", Type::Int8), ("b", Type::Int8)]),
			key_strategy: KeyStrategy::Sequential,
			output_key_columns: vec!["a".into(), "b".into()],
		};
		assert!(schema.validate().is_ok());
	}
}
