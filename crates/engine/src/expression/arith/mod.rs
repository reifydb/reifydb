// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

/// Generates a complete match expression dispatching all numeric type pairs to the appropriate
/// handler functions. Uses push-down accumulation to build the cross-product of type arms.
///
/// Fixed-width pairs (12×12=144 arms) use `$fh`, while any pair involving arbitrary-precision
/// types (81 arms) uses `$ah`. Additional match arms are appended via `$($extra:tt)*`.
macro_rules! dispatch_arith {
	// Entry point
	(
		$left:expr, $right:expr;
		fixed: $fh:ident, arb: $ah:ident ($ctx:expr, $target:expr, $fragment:expr);
		$($extra:tt)*
	) => {
		dispatch_arith!(@rows
			($left, $right) $fh $ah ($ctx, $target, $fragment)
			[Float4 Float8 Int1 Int2 Int4 Int8 Int16 Uint1 Uint2 Uint4 Uint8 Uint16]
			{$($extra)*}
			{}
		)
	};

	// Recursive: process one fixed-left type, generating all 15 right-side arms
	(@rows
		($left:expr, $right:expr) $fh:ident $ah:ident ($ctx:expr, $target:expr, $fragment:expr)
		[$L:ident $($rest:ident)*]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		dispatch_arith!(@rows
			($left, $right) $fh $ah ($ctx, $target, $fragment)
			[$($rest)*]
			{$($extra)*}
			{
				$($acc)*
				(ColumnData::$L(l), ColumnData::Float4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Float8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int1(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int2(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int16(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint1(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint2(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint16(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
				(ColumnData::$L(l), ColumnData::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			}
		)
	};

	// Base case: all fixed-left types processed, emit the match with arb-left arms
	(@rows
		($left:expr, $right:expr) $fh:ident $ah:ident ($ctx:expr, $target:expr, $fragment:expr)
		[]
		{$($extra:tt)*}
		{$($acc:tt)*}
	) => {
		match ($left, $right) {
			// Fixed × Fixed + Fixed × Arb (12 × 15 = 180 arms)
			$($acc)*

			// Arb × all (3 × 15 = 45 arms)
			(ColumnData::Int { container: l, .. }, ColumnData::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Int { container: l, .. }, ColumnData::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

			(ColumnData::Uint { container: l, .. }, ColumnData::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Uint { container: l, .. }, ColumnData::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

			(ColumnData::Decimal { container: l, .. }, ColumnData::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnData::Decimal { container: l, .. }, ColumnData::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

			// Additional arms (special cases, undefined handling, error fallback)
			$($extra)*
		}
	};
}

pub mod add;
pub mod div;
pub mod mul;
pub mod rem;
pub mod sub;
