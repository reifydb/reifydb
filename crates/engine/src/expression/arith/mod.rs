// SPDX-License-Identifier: Apache-2.0
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
				(ColumnBuffer::$L(l), ColumnBuffer::Float4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Float8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int1(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int2(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int16(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint1(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint2(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint4(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint8(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint16(r)) => $fh($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
				(ColumnBuffer::$L(l), ColumnBuffer::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
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
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Int { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Uint { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Float8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint1(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint2(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint4(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint8(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint16(r)) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Int { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Uint { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),
			(ColumnBuffer::Decimal { container: l, .. }, ColumnBuffer::Decimal { container: r, .. }) => $ah($ctx, l, r, $target, $fragment),

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
