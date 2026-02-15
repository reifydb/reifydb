// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstDispatch, identifier::DispatchTargetIdentifier, parse::Parser},
	token::keyword::Keyword,
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_dispatch(&mut self) -> crate::Result<AstDispatch<'bump>> {
		let token = self.consume_keyword(Keyword::Dispatch)?;

		// Parse dot-separated target (e.g. app.wallet.deposit)
		let segments = self.parse_dot_separated_identifiers()?;
		let target = DispatchTargetIdentifier::new(segments.into_iter().map(|s| s.into_fragment()).collect());

		// Parse inline payload { field: expr }
		let payload = self.parse_inline()?;

		Ok(AstDispatch {
			token,
			target,
			payload,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_dispatch_three_segment_target() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"DISPATCH app.wallet.deposit { amount: 100 }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let node = result.first_unchecked();
		assert!(node.is_dispatch());

		let dispatch = node.as_dispatch();
		assert_eq!(dispatch.target.segments.len(), 3);
		assert_eq!(dispatch.target.segments[0].text(), "app");
		assert_eq!(dispatch.target.segments[1].text(), "wallet");
		assert_eq!(dispatch.target.segments[2].text(), "deposit");
		assert_eq!(dispatch.payload.len(), 1);
		assert_eq!(dispatch.payload[0].key.text(), "amount");
	}

	#[test]
	fn test_dispatch_two_segment_target() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, r#"DISPATCH counter.increment { step: 1 }"#).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let dispatch = result.first_unchecked().as_dispatch();
		assert_eq!(dispatch.target.segments.len(), 2);
		assert_eq!(dispatch.target.segments[0].text(), "counter");
		assert_eq!(dispatch.target.segments[1].text(), "increment");
		assert_eq!(dispatch.payload.len(), 1);
	}

	#[test]
	fn test_dispatch_multi_field_payload() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"DISPATCH app.transfer.execute { from_account: 'acc1', to_account: 'acc2', amount: 500 }"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let dispatch = result.first_unchecked().as_dispatch();
		assert_eq!(dispatch.target.segments.len(), 3);
		assert_eq!(dispatch.payload.len(), 3);
		assert_eq!(dispatch.payload[0].key.text(), "from_account");
		assert_eq!(dispatch.payload[1].key.text(), "to_account");
		assert_eq!(dispatch.payload[2].key.text(), "amount");
	}

	#[test]
	fn test_dispatch_is_ddl() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"DISPATCH app.wallet.deposit { amount: 100 }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let mut result = parser.parse().unwrap();
		assert_eq!(result.len(), 1);

		let result = result.pop().unwrap();
		let node = result.first_unchecked();
		// Dispatch is treated as DDL (standalone statement)
		assert!(node.is_ddl());
	}
}
