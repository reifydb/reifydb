// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Write;

pub fn values_placeholders(rows: usize, cols: usize) -> String {
	let mut sql = String::with_capacity(rows * (cols * 2 + 2));
	let mut n = 1usize;
	for r in 0..rows {
		if r > 0 {
			sql.push(',');
		}
		sql.push('(');
		for c in 0..cols {
			if c > 0 {
				sql.push(',');
			}
			write!(sql, "?{n}").unwrap();
			n += 1;
		}
		sql.push(')');
	}
	sql
}

#[cfg(test)]
mod tests {
	use super::values_placeholders;

	#[test]
	fn one_row_one_column_is_a_single_placeholder() {
		assert_eq!(values_placeholders(1, 1), "(?1)");
	}

	#[test]
	fn one_row_three_columns_numbers_left_to_right() {
		assert_eq!(values_placeholders(1, 3), "(?1,?2,?3)");
	}

	#[test]
	fn three_rows_two_columns_numbers_contiguously_across_rows() {
		// callers rely on contiguous numbering to bind a single flat parameter list per chunk
		assert_eq!(values_placeholders(3, 2), "(?1,?2),(?3,?4),(?5,?6)");
	}

	#[test]
	fn zero_rows_is_empty() {
		assert_eq!(values_placeholders(0, 3), "");
	}
}
