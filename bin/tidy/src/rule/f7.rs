// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{
	ExprLet, Item, Local, Pat, PatIdent,
	visit::{self, Visit},
};

use crate::rule::Function;

const LIMIT: usize = 10;

pub(crate) fn breaks(function: &Function) -> bool {
	let Some(body) = function.body else {
		return false;
	};
	let mut locals = Locals {
		count: 0,
	};
	locals.visit_block(body);
	locals.count > LIMIT
}

struct Locals {
	count: usize,
}

impl<'ast> Visit<'ast> for Locals {
	fn visit_item(&mut self, _item: &'ast Item) {}

	fn visit_local(&mut self, node: &'ast Local) {
		self.count += names(&node.pat);
		if let Some(init) = &node.init {
			self.visit_local_init(init);
		}
	}

	fn visit_expr_let(&mut self, node: &'ast ExprLet) {
		self.count += names(&node.pat);
		self.visit_expr(&node.expr);
	}
}

fn names(pattern: &Pat) -> usize {
	let mut names = Names {
		count: 0,
	};
	names.visit_pat(pattern);
	names.count
}

struct Names {
	count: usize,
}

impl<'ast> Visit<'ast> for Names {
	fn visit_pat_ident(&mut self, node: &'ast PatIdent) {
		if node.ident.to_string().starts_with(|first: char| first.is_lowercase() || first == '_') {
			self.count += 1;
		}
		visit::visit_pat_ident(self, node);
	}
}
