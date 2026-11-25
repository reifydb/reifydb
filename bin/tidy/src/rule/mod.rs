// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod f4;
pub mod f5;

use syn::{
	Abi, Error, ForeignItemFn, ImplItemFn, ItemFn, ItemForeignMod, ItemImpl, Signature, TraitItemFn, parse_file,
	visit::{self, Visit},
};

pub struct Function<'ast> {
	pub signature: &'ast Signature,
	pub abi: Option<&'ast Abi>,
}

pub struct Breach {
	pub rule: &'static str,
	pub line: usize,
	pub function: String,
}

type Rule = fn(&Function) -> bool;

const RULES: [(&str, Rule); 2] = [("F4", f4::breaks), ("F5", f5::breaks)];

pub fn check(text: &str) -> Result<Vec<Breach>, Error> {
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

impl<'ast> Visit<'ast> for Checker<'ast> {
	fn visit_item_fn(&mut self, item: &'ast ItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: item.sig.abi.as_ref(),
		});
		visit::visit_item_fn(self, item);
	}

	fn visit_trait_item_fn(&mut self, item: &'ast TraitItemFn) {
		self.check_function(Function {
			signature: &item.sig,
			abi: item.sig.abi.as_ref(),
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
		if self.signature_owner == SignatureOwner::Function {
			self.check_function(Function {
				signature: &item.sig,
				abi: item.sig.abi.as_ref(),
			});
		}
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
		});
		visit::visit_foreign_item_fn(self, item);
	}
}

#[cfg(test)]
mod guide;

#[cfg(test)]
mod tests {
	use super::check;

	fn rules(text: &str) -> Vec<&'static str> {
		check(text).unwrap().iter().map(|breach| breach.rule).collect()
	}

	#[test]
	fn four_arguments_pass_and_five_break_f4() {
		// The limit must sit exactly at 4, otherwise the gate drifts by one in either direction.
		assert!(rules("fn scan(first: u8, second: u8, third: u8, fourth: u8) {}").is_empty());
		assert_eq!(rules("fn scan(first: u8, second: u8, third: u8, fourth: u8, fifth: u8) {}"), ["F4"]);
	}

	#[test]
	fn the_receiver_counts_as_an_argument() {
		// Without counting self, a method could take one argument more than a free function.
		assert!(rules("impl Store { fn scan(&self, first: u8, second: u8, third: u8) {} }").is_empty());
		assert_eq!(
			rules("impl Store { fn scan(&self, first: u8, second: u8, third: u8, fourth: u8) {} }"),
			["F4"]
		);
	}

	#[test]
	fn extern_c_functions_are_exempt_from_f4_only() {
		// The exemption must never widen to F5 or to other ABIs, otherwise extern becomes an opt-out marker.
		let crowded = "first: u8, second: u8, third: u8, fourth: u8, fifth: u8";
		assert!(rules(&format!("extern \"C\" fn hook({crowded}) {{}}")).is_empty());
		assert!(rules(&format!("extern \"C\" {{ fn hook({crowded}); }}")).is_empty());
		assert!(rules(&format!("extern fn hook({crowded}) {{}}")).is_empty());
		assert_eq!(rules(&format!("extern \"Rust\" fn hook({crowded}) {{}}")), ["F4"]);
		assert_eq!(rules("extern \"C\" fn hook(armed: bool) {}"), ["F5"]);
	}

	#[test]
	fn a_trait_owns_its_signature_and_its_impls_are_not_counted_again() {
		// Counting the impl copies would report one breach once per implementor, which no implementor can fix.
		let crowded = "&self, first: u8, second: u8, third: u8, fourth: u8";
		assert_eq!(rules(&format!("trait Scan {{ fn scan({crowded}); }}")), ["F4"]);
		assert!(rules(&format!("impl Scan for Store {{ fn scan({crowded}) {{}} }}")).is_empty());
	}

	#[test]
	fn a_function_nested_in_a_trait_impl_method_is_still_checked() {
		// The trait exemption must end at the method signature, otherwise a body becomes a hiding place.
		let text = "impl Scan for Store { fn scan(&self) { fn inner(armed: bool) {} } }";
		assert_eq!(rules(text), ["F5"]);
	}

	#[test]
	fn an_inherent_impl_after_a_trait_impl_is_checked() {
		// The trait exemption must be restored after the impl block, otherwise it leaks to the rest of the
		// file.
		let text = "impl Scan for Store { fn scan(&self) {} } impl Store { fn arm(&self, armed: bool) {} }";
		assert_eq!(rules(text), ["F5"]);
	}

	#[test]
	fn a_plain_bool_argument_breaks_f5_whatever_the_visibility() {
		// F5 has no exceptions, so a private function or a method must never slip through.
		assert_eq!(rules("fn scan(reverse: bool) {}"), ["F5"]);
		assert_eq!(rules("pub fn scan(reverse: bool) {}"), ["F5"]);
		assert_eq!(rules("impl Store { fn scan(&self, reverse: bool) {} }"), ["F5"]);
		assert!(rules("fn scan(order: Order) -> bool { true }").is_empty());
	}

	#[test]
	fn a_bool_behind_a_reference_or_wrapper_passes_f5() {
		// Pins the known gap: only a plain bool is flagged, and this must fail loudly if that ever changes.
		assert!(rules("fn scan(reverse: &bool) {}").is_empty());
		assert!(rules("fn scan(reverse: Option<bool>) {}").is_empty());
	}

	#[test]
	fn one_function_can_break_both_rules() {
		// Each rule must report on its own, otherwise the first breach hides the second from the baseline.
		let text = "fn scan(first: u8, second: u8, third: u8, fourth: u8, reverse: bool) {}";
		assert_eq!(rules(text), ["F4", "F5"]);
	}

	#[test]
	fn a_breach_names_the_function_and_its_line() {
		// The report is only usable if it points at the exact function, never at the file start.
		let found = check("fn first() {}\n\nfn second(reverse: bool) {}\n").unwrap();
		assert_eq!(found.len(), 1);
		assert_eq!((found[0].line, found[0].function.as_str()), (3, "second"));
	}

	#[test]
	fn text_that_does_not_parse_is_an_error() {
		// A file that cannot be read must stop the run, never count as zero breaches.
		assert!(check("fn scan(").is_err());
	}
}
