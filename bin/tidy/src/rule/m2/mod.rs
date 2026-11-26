// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{
	Attribute, File, Ident, ImplItemFn, Item, ItemFn, ItemImpl, ItemMod,
	visit::{self, Visit},
};

use crate::rule::SourceFile;

struct Finder {
	lines: Vec<usize>,
}

pub(crate) fn breaches(file: &SourceFile) -> Vec<usize> {
	if file.is_test_file() {
		return Vec::new();
	}
	let mut finder = Finder {
		lines: Vec::new(),
	};
	finder.visit_file(file.tree);
	finder.lines.extend(breaches_find_misplaced(file.tree));
	finder.lines
}

fn breaches_find_misplaced(tree: &File) -> Option<usize> {
	let position = tree.items.iter().position(is_tests_item)?;
	if position + 1 == tree.items.len() {
		return None;
	}
	let Item::Mod(module) = &tree.items[position] else {
		return None;
	};
	Some(module.mod_token.span.start().line)
}

fn is_tests_item(item: &Item) -> bool {
	let Item::Mod(module) = item else {
		return false;
	};
	is_tests_declaration(module)
}

fn is_tests_declaration(module: &ItemMod) -> bool {
	module.content.is_none() && module.ident == "tests" && find_marker_line(&module.attrs).is_some()
}

fn find_marker_line(attributes: &[Attribute]) -> Option<usize> {
	let marker = attributes.iter().find(|attribute| is_test_marker(attribute))?;
	Some(marker.pound_token.span.start().line)
}

fn is_test_marker(attribute: &Attribute) -> bool {
	let path = attribute.path();
	if path.is_ident("cfg") {
		return attribute.parse_args::<Ident>().is_ok_and(|ident| ident == "test");
	}
	path.segments.last().is_some_and(|segment| segment.ident == "test")
}

impl<'ast> Visit<'ast> for Finder {
	fn visit_item_mod(&mut self, item: &'ast ItemMod) {
		if is_tests_declaration(item) {
			return;
		}
		if let Some(line) = find_marker_line(&item.attrs) {
			self.lines.push(line);
			return;
		}
		visit::visit_item_mod(self, item);
	}

	fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
		if let Some(line) = find_marker_line(&item.attrs) {
			self.lines.push(line);
			return;
		}
		visit::visit_item_impl(self, item);
	}

	fn visit_item_fn(&mut self, item: &'ast ItemFn) {
		if let Some(line) = find_marker_line(&item.attrs) {
			self.lines.push(line);
			return;
		}
		visit::visit_item_fn(self, item);
	}

	fn visit_impl_item_fn(&mut self, item: &'ast ImplItemFn) {
		if let Some(line) = find_marker_line(&item.attrs) {
			self.lines.push(line);
			return;
		}
		visit::visit_impl_item_fn(self, item);
	}

	fn visit_attribute(&mut self, attribute: &'ast Attribute) {
		if is_test_marker(attribute) {
			self.lines.push(attribute.pound_token.span.start().line);
		}
	}
}

#[cfg(test)]
mod tests;
