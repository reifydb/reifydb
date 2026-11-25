// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod f2;
mod f3;
mod f4;
mod f5;
mod f7;

use syn::{
	Abi, Block, Error, ForeignItemFn, ImplItemFn, ItemFn, ItemForeignMod, ItemImpl, Signature, TraitItemFn,
	parse_file,
	visit::{self, Visit},
};

pub(crate) struct Function<'ast> {
	signature: &'ast Signature,
	abi: Option<&'ast Abi>,
	body: Option<&'ast Block>,
	signature_owner: SignatureOwner,
}

pub(crate) struct Breach {
	pub(crate) rule: &'static str,
	pub(crate) line: usize,
	pub(crate) function: String,
}

type Rule = fn(&Function) -> bool;

const RULES: [(&str, Rule); 5] =
	[("F2", f2::breaks), ("F3", f3::breaks), ("F4", f4::breaks), ("F5", f5::breaks), ("F7", f7::breaks)];

pub(crate) fn check(text: &str) -> Result<Vec<Breach>, Error> {
	let file = parse_file(text)?;
	let mut checker = Checker {
		breaches: Vec::new(),
		signature_owner: SignatureOwner::Function,
		foreign_abi: None,
	};
	checker.visit_file(&file);
	Ok(checker.breaches)
}

#[derive(Clone, Copy, PartialEq)]
enum SignatureOwner {
	Function,
	Trait,
}

struct Checker<'ast> {
	breaches: Vec<Breach>,
	signature_owner: SignatureOwner,
	foreign_abi: Option<&'ast Abi>,
}

impl<'ast> Visit<'ast> for Checker<'ast> {
	fn visit_item_fn(&mut self, item: &'ast ItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: item.sig.abi.as_ref(),
			body: Some(&item.block),
			signature_owner: SignatureOwner::Function,
		});
		visit::visit_item_fn(self, item);
	}

	fn visit_trait_item_fn(&mut self, item: &'ast TraitItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: item.sig.abi.as_ref(),
			body: item.default.as_ref(),
			signature_owner: SignatureOwner::Function,
		});
		visit::visit_trait_item_fn(self, item);
	}

	fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
		let outer = self.signature_owner;
		self.signature_owner = match item.trait_ {
			Some(_) => SignatureOwner::Trait,
			None => SignatureOwner::Function,
		};
		visit::visit_item_impl(self, item);
		self.signature_owner = outer;
	}

	fn visit_impl_item_fn(&mut self, item: &'ast ImplItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: item.sig.abi.as_ref(),
			body: Some(&item.block),
			signature_owner: self.signature_owner,
		});
		visit::visit_impl_item_fn(self, item);
	}

	fn visit_item_foreign_mod(&mut self, item: &'ast ItemForeignMod) {
		let outer = self.foreign_abi;
		self.foreign_abi = Some(&item.abi);
		visit::visit_item_foreign_mod(self, item);
		self.foreign_abi = outer;
	}

	fn visit_foreign_item_fn(&mut self, item: &'ast ForeignItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: self.foreign_abi,
			body: None,
			signature_owner: SignatureOwner::Function,
		});
		visit::visit_foreign_item_fn(self, item);
	}
}

impl<'ast> Checker<'ast> {
	fn check_function(&mut self, function: Function<'ast>) {
		for (rule, breaks) in RULES {
			if !breaks(&function) {
				continue;
			}
			self.breaches.push(Breach {
				rule,
				line: function.signature.ident.span().start().line,
				function: function.signature.ident.to_string(),
			});
		}
	}
}

#[cfg(test)]
mod tests;
