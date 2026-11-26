// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{VisRestricted, visit::Visit};

use crate::rule::SourceFile;

struct Finder {
	lines: Vec<usize>,
}

pub(crate) fn breaches(file: &SourceFile) -> Vec<usize> {
	let mut finder = Finder {
		lines: Vec::new(),
	};
	finder.visit_file(file.tree);
	finder.lines
}

impl<'ast> Visit<'ast> for Finder {
	fn visit_vis_restricted(&mut self, restricted: &'ast VisRestricted) {
		if restricted.in_token.is_none() && restricted.path.is_ident("crate") {
			return;
		}
		self.lines.push(restricted.pub_token.span.start().line);
	}
}

#[cfg(test)]
mod tests;
