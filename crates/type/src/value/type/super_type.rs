// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::value::r#type::Type;

impl Type {
	/// Returns the widest `Type` that losslessly accommodates every input,
	/// reducing via `Type::promote`.
	///
	/// Returns `Type::Any` for an empty input (matches the fallback that
	/// `ColumnData::with_capacity` uses when no type is known).
	pub fn super_type_of<I>(iter: I) -> Type
	where
		I: IntoIterator<Item = Type>,
	{
		let mut iter = iter.into_iter();
		match iter.next() {
			Some(first) => iter.fold(first, Type::promote),
			None => Type::Any,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::iter;

	use Type::*;

	use crate::value::r#type::Type;

	fn opt(inner: Type) -> Type {
		Option(Box::new(inner))
	}

	#[test]
	fn empty_input_returns_any() {
		assert_eq!(Type::super_type_of(Vec::<Type>::new()), Any);
	}

	#[test]
	fn single_element_passes_through_for_all_representative_types() {
		let cases = [
			Boolean,
			Float4,
			Float8,
			Int1,
			Int2,
			Int4,
			Int8,
			Int16,
			Uint1,
			Uint2,
			Uint4,
			Uint8,
			Uint16,
			Utf8,
			Date,
			DateTime,
			Time,
			Duration,
			Uuid4,
			Uuid7,
			Blob,
			IdentityId,
			DictionaryId,
			Int,
			Uint,
			Decimal,
			Any,
			opt(Int4),
		];
		for ty in cases {
			let input = [ty.clone()];
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, ty, "single-element identity failed for {:?}", ty);
		}
	}

	#[test]
	fn pair_grid() {
		let cases: &[(Type, Type, Type)] = &[
			(Int1, Int1, Int2),
			(Int2, Int2, Int4),
			(Int4, Int4, Int8),
			(Int8, Int8, Int16),
			(Int16, Int16, Int16),
			(Int1, Int2, Int4),
			(Int1, Int4, Int8),
			(Int1, Int8, Int16),
			(Int1, Int16, Int16),
			(Int2, Int4, Int8),
			(Int2, Int8, Int16),
			(Int4, Int8, Int16),
			(Uint1, Uint1, Uint2),
			(Uint2, Uint2, Uint4),
			(Uint4, Uint4, Uint8),
			(Uint8, Uint8, Uint16),
			(Uint16, Uint16, Uint16),
			(Uint1, Uint4, Uint8),
			(Uint4, Uint8, Uint16),
			(Uint1, Int1, Int2),
			(Int1, Uint1, Int2),
			(Uint4, Int4, Int8),
			(Int4, Uint4, Int8),
			(Uint4, Int8, Int16),
			(Int4, Uint8, Int16),
			(Int8, Uint4, Int16),
			(Uint16, Int1, Int16),
			(Int1, Uint16, Int16),
			(Float4, Float4, Float8),
			(Float4, Float8, Float8),
			(Float8, Float4, Float8),
			(Float8, Float8, Float8),
			(Float4, Int1, Float8),
			(Float4, Int8, Float8),
			(Float4, Uint4, Float8),
			(Float8, Int16, Float8),
			(Boolean, Boolean, Boolean),
			(Boolean, Int4, Boolean),
			(Int4, Boolean, Boolean),
			(Boolean, Float4, Boolean),
			(Boolean, Float8, Boolean),
			(Boolean, Uint8, Boolean),
			(Utf8, Utf8, Utf8),
			(Utf8, Boolean, Utf8),
			(Boolean, Utf8, Utf8),
			(Utf8, Int4, Utf8),
			(Int4, Utf8, Utf8),
			(Utf8, Float8, Utf8),
			(Utf8, Uint16, Utf8),
		];
		for (l, r, expected) in cases {
			let got = Type::super_type_of([l.clone(), r.clone()]);
			assert_eq!(got, *expected, "pair [{:?}, {:?}] expected {:?} got {:?}", l, r, expected, got);
		}
	}

	#[test]
	fn triple_fold() {
		let cases: &[(Type, Type, Type, Type)] = &[
			(Int1, Int1, Int1, Int4),
			(Int1, Int2, Int4, Int8),
			(Int1, Int4, Int8, Int16),
			(Int1, Utf8, Int4, Utf8),
			(Int4, Boolean, Float8, Boolean),
			(Int2, Int2, Int2, Int8),
			(Uint1, Uint1, Uint1, Uint4),
			(Float4, Int4, Int2, Float8),
		];
		for (a, b, c, expected) in cases {
			let got = Type::super_type_of([a.clone(), b.clone(), c.clone()]);
			assert_eq!(
				got, *expected,
				"triple [{:?}, {:?}, {:?}] expected {:?} got {:?}",
				a, b, c, expected, got
			);
		}
	}

	#[test]
	fn accepts_vec_input() {
		let input: Vec<Type> = vec![Int1, Int1];
		assert_eq!(Type::super_type_of(input), Int2);
	}

	#[test]
	fn accepts_slice_iter_cloned_input() {
		let input: &[Type] = &[Int1, Int1];
		assert_eq!(Type::super_type_of(input.iter().cloned()), Int2);
	}

	#[test]
	fn signed_progressive_widening() {
		let cases: &[(Vec<Type>, Type)] = &[
			(vec![Int1, Int1, Int1], Int4),
			(vec![Int1, Int1, Int1, Int1], Int8),
			(vec![Int1, Int1, Int1, Int1, Int1], Int16),
			(vec![Int1, Int1, Int1, Int1, Int1, Int1], Int16),
		];
		for (input, expected) in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, *expected, "input {:?} expected {:?} got {:?}", input, expected, got);
		}
	}

	#[test]
	fn unsigned_progressive_widening() {
		let cases: &[(Vec<Type>, Type)] = &[
			(vec![Uint1, Uint1, Uint1], Uint4),
			(vec![Uint1, Uint1, Uint1, Uint1], Uint8),
			(vec![Uint1, Uint1, Uint1, Uint1, Uint1], Uint16),
			(vec![Uint1, Uint1, Uint1, Uint1, Uint1, Uint1], Uint16),
		];
		for (input, expected) in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, *expected, "input {:?} expected {:?} got {:?}", input, expected, got);
		}
	}

	#[test]
	fn widening_saturates_at_int16() {
		let cases: &[(Vec<Type>, Type)] = &[
			(vec![Int16, Int1, Int4, Int8], Int16),
			(vec![Int16, Int16, Int16], Int16),
			(vec![Int8, Int8, Int8, Int8], Int16),
		];
		for (input, expected) in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, *expected, "input {:?} expected {:?} got {:?}", input, expected, got);
		}
	}

	#[test]
	fn widening_saturates_at_uint16() {
		let cases: &[(Vec<Type>, Type)] = &[
			(vec![Uint16, Uint1, Uint4, Uint8], Uint16),
			(vec![Uint16, Uint16, Uint16], Uint16),
			(vec![Uint8, Uint8, Uint8, Uint8], Uint16),
		];
		for (input, expected) in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, *expected, "input {:?} expected {:?} got {:?}", input, expected, got);
		}
	}

	#[test]
	fn float_presence_dominates() {
		let cases: &[(Vec<Type>, Type)] = &[
			(vec![Int1, Int2, Float4], Float8),
			(vec![Float4, Int1, Int2], Float8),
			(vec![Int1, Float8, Uint4], Float8),
		];
		for (input, expected) in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, *expected, "input {:?} expected {:?} got {:?}", input, expected, got);
		}
	}

	#[test]
	fn utf8_presence_dominates() {
		let cases: &[Vec<Type>] = &[
			vec![Int1, Int2, Utf8],
			vec![Utf8, Int1, Int2],
			vec![Utf8, Utf8, Utf8],
			vec![Uint16, Utf8, Int4],
		];
		for input in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, Utf8, "input {:?} expected Utf8 got {:?}", input, got);
		}
	}

	#[test]
	fn boolean_presence_dominates_over_numeric() {
		let cases: &[Vec<Type>] =
			&[vec![Boolean, Int1, Int2], vec![Int1, Boolean, Int4], vec![Boolean, Boolean, Boolean]];
		for input in cases {
			let got = Type::super_type_of(input.iter().cloned());
			assert_eq!(got, Boolean, "input {:?} expected Boolean got {:?}", input, got);
		}
	}

	#[test]
	fn utf8_dominates_boolean() {
		assert_eq!(Type::super_type_of([Boolean, Utf8, Int4]), Utf8);
		assert_eq!(Type::super_type_of([Int4, Boolean, Utf8]), Utf8);
		assert_eq!(Type::super_type_of([Utf8, Boolean, Boolean]), Utf8);
	}

	#[test]
	fn option_on_left_short_circuits() {
		assert_eq!(Type::super_type_of([opt(Int4), Int1]), opt(Int4));
		assert_eq!(Type::super_type_of([opt(Int4), Int16]), opt(Int4));
		assert_eq!(Type::super_type_of([opt(Utf8), Boolean]), opt(Utf8));
		assert_eq!(Type::super_type_of([opt(opt(Int4)), Int1]), opt(opt(Int4)));
		assert_eq!(Type::super_type_of([opt(Int4), Int1, Int2]), opt(Int4));
	}

	#[test]
	fn option_on_right_short_circuits_to_left() {
		assert_eq!(Type::super_type_of([Int1, opt(Int4)]), Int1);
		assert_eq!(Type::super_type_of([Int16, opt(Int4)]), Int16);
		assert_eq!(Type::super_type_of([Boolean, opt(Utf8)]), Boolean);
		assert_eq!(Type::super_type_of([Utf8, opt(Int4)]), Utf8);
	}

	#[test]
	fn non_numeric_pass_through() {
		let cases = [
			Date,
			DateTime,
			Time,
			Duration,
			Uuid4,
			Uuid7,
			Blob,
			IdentityId,
			DictionaryId,
			Decimal,
			Int,
			Uint,
		];
		for ty in cases {
			let got = Type::super_type_of([ty.clone(), ty.clone()]);
			assert_eq!(got, ty, "pass-through failed for {:?}", ty);
			let got3 = Type::super_type_of([ty.clone(), ty.clone(), ty.clone()]);
			assert_eq!(got3, ty, "pass-through triple failed for {:?}", ty);
		}
	}

	#[test]
	fn mixed_integer_widths_regression() {
		// The exact mixed-width integer shape produced by the scalar VM across rows
		// in the per-row UDF fallback path at
		// `crates/engine/src/vm/exec/call.rs::run_function_body_per_row`.
		// Regression guard: if this returns anything narrower than Int16, the
		// accumulator column assembly will re-hit the `push_value` unimplemented
		// panic at `crates/core/src/value/column/push/value.rs`.
		let input = [Int16, Int16, Int16, Int8, Int1];
		assert_eq!(Type::super_type_of(input), Int16);
	}

	#[test]
	fn any_is_absorbing() {
		// Any is the top of the type lattice: whenever it appears in
		// the input, the result is Any — regardless of position or
		// what else it's paired with, including types that otherwise
		// dominate (Utf8, Boolean, Float*) or short-circuit (Option).
		let others = [Int4, Uint8, Utf8, Boolean, Float4, Float8, Date, Uuid4, Blob, opt(Int4)];
		for ty in &others {
			assert_eq!(Type::super_type_of([Any, ty.clone()]), Any, "[Any, {:?}]", ty);
			assert_eq!(Type::super_type_of([ty.clone(), Any]), Any, "[{:?}, Any]", ty);
		}
		// Any in the middle and at the tail still wins; multiple Anys stay Any.
		assert_eq!(Type::super_type_of([Int4, Any, Int2]), Any);
		assert_eq!(Type::super_type_of([Int1, Int2, Any]), Any);
		assert_eq!(Type::super_type_of([Any, Any, Int4]), Any);
	}

	#[test]
	fn large_input_stable() {
		// Saturates to Int16 after 5 fold steps; remaining 995 steps stay at Int16.
		let input: Vec<Type> = iter::repeat(Int1).take(1000).collect();
		assert_eq!(Type::super_type_of(input), Int16);
	}
}
