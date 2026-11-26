// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::SourceFile;

const LINES_MAX: usize = 500;

pub(crate) fn breaches(file: &SourceFile) -> Vec<usize> {
	if file.is_test_file() || file.text.lines().count() <= LINES_MAX {
		return Vec::new();
	}
	vec![LINES_MAX + 1]
}

#[cfg(test)]
mod tests;
