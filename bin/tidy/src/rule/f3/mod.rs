// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{
	Expr::{Block, If},
	ExprClosure, ExprForLoop, ExprIf, ExprLoop, ExprMatch, ExprWhile, Item,
	visit::Visit,
};

use crate::rule::Function;

const LIMIT: usize = 3;

pub(crate) fn breaks(function: &Function) -> bool {
	let Some(body) = function.body else {
		return false;
	};
	let mut depth = Depth {
		current: 0,
		deepest: 0,
	};
	depth.visit_block(body);
	depth.deepest > LIMIT
}

struct Depth {
	current: usize,
	deepest: usize,
}

impl<'ast> Visit<'ast> for Depth {
	fn visit_item(&mut self, _item: &'ast Item) {}

	fn visit_expr_if(&mut self, node: &'ast ExprIf) {
		self.visit_expr(&node.cond);
		self.enter();
		self.visit_block(&node.then_branch);
		self.leave();
		let Some((_, otherwise)) = &node.else_branch else {
			return;
		};
		if let If(chained) = otherwise.as_ref() {
			self.visit_expr_if(chained);
			return;
		}
		self.enter();
		self.visit_expr(otherwise);
		self.leave();
	}

	fn visit_expr_match(&mut self, node: &'ast ExprMatch) {
		self.visit_expr(&node.expr);
		self.enter();
		for arm in &node.arms {
			self.visit_arm(arm);
		}
		self.leave();
	}

	fn visit_expr_for_loop(&mut self, node: &'ast ExprForLoop) {
		self.visit_expr(&node.expr);
		self.enter();
		self.visit_block(&node.body);
		self.leave();
	}

	fn visit_expr_while(&mut self, node: &'ast ExprWhile) {
		self.visit_expr(&node.cond);
		self.enter();
		self.visit_block(&node.body);
		self.leave();
	}

	fn visit_expr_loop(&mut self, node: &'ast ExprLoop) {
		self.enter();
		self.visit_block(&node.body);
		self.leave();
	}

	fn visit_expr_closure(&mut self, node: &'ast ExprClosure) {
		if !matches!(node.body.as_ref(), Block(_)) {
			self.visit_expr(&node.body);
			return;
		}
		self.enter();
		self.visit_expr(&node.body);
		self.leave();
	}
}

impl Depth {
	fn enter(&mut self) {
		self.current += 1;
		self.deepest = self.deepest.max(self.current);
	}

	fn leave(&mut self) {
		self.current -= 1;
	}
}
