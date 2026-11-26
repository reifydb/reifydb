// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::SourceFile;

pub(crate) fn breaches(file: &SourceFile) -> Vec<usize> {
	if file.path.with_extension("").is_dir() {
		return vec![1];
	}
	Vec::new()
}

#[cfg(test)]
mod tests;
