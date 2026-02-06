// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstInline, AstInlineKeyedValue},
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	token::{
		operator::{
			Operator,
			Operator::{CloseCurly, Colon},
		},
		separator::Separator::Comma,
		token::TokenKind,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_inline(&mut self) -> crate::Result<AstInline<'bump>> {
		let token = self.consume_operator(Operator::OpenCurly)?;

		let mut keyed_values = Vec::with_capacity(4);
		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			let key = self.parse_identifier_with_hyphens()?;
			self.consume_operator(Colon)?;
			let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

			keyed_values.push(AstInlineKeyedValue {
				key,
				value,
			});

			self.skip_new_line()?;
			self.consume_if(TokenKind::Separator(Comma))?;
		}

		self.consume_operator(CloseCurly)?;
		Ok(AstInline {
			token,
			keyed_values,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{
		ast::{
			ast::{
				Ast::{Identifier, Literal},
				AstLiteral::{Number, Text},
			},
			parse::parse,
		},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_empty_inline() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 0);
	}

	#[test]
	fn test_single_keyed_value() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{id: 1}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.text(), "id");
		let Literal(Number(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "1");
	}

	#[test]
	fn test_keyword() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{value: 1}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.text(), "value");
		let Literal(Number(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "1");
	}

	#[test]
	fn test_text() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"{text: 'Ada'}"#).unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.text(), "text");
		let Literal(Text(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "Ada");
	}

	#[test]
	fn test_multiple_keyed_values() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"{id: 1, name: 'Ada'}"#).unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.text(), "id");
		let Literal(Number(value)) = id_keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "1");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.text(), "name");
		let Literal(Text(value)) = name_keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "Ada");
	}

	#[test]
	fn test_identifier_value() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{keyed_value: someVariable}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.text(), "keyed_value");
		let Identifier(identifier) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.text(), "someVariable");
	}

	#[test]
	fn test_multiline_inline() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"{
            id: 42,
            name: 'Database',
            active: true
        }"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 3);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.text(), "id");
		let Literal(Number(value)) = id_keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "42");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.text(), "name");
		let Literal(Text(value)) = name_keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "Database");

		let active_keyed_value = &inline[2];
		assert_eq!(active_keyed_value.key.text(), "active");
		assert!(active_keyed_value.value.is_literal_boolean());
	}

	#[test]
	fn test_trailing_comma() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{id: 1, name: 'Test'}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.text(), "id");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.text(), "name");
	}

	#[test]
	fn test_comptokenize_values() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{result: (1 + 2), enabled: !false}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let result_keyed_value = &inline[0];
		assert_eq!(result_keyed_value.key.text(), "result");
		assert!(result_keyed_value.value.is_tuple());

		let enabled_keyed_value = &inline[1];
		assert_eq!(enabled_keyed_value.key.text(), "enabled");
		assert!(enabled_keyed_value.value.is_prefix());
	}

	#[test]
	fn test_nested_inline() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, "{user: {id: 1, name: 'John'}}").unwrap().into_iter().collect();
		let result = parse(&bump, tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let user_keyed_value = &inline[0];
		assert_eq!(user_keyed_value.key.text(), "user");
		assert!(user_keyed_value.value.is_block());

		let nested_inline = user_keyed_value.value.as_block();
		assert_eq!(nested_inline.len(), 2);
		assert_eq!(nested_inline[0].key.text(), "id");
		assert_eq!(nested_inline[1].key.text(), "name");
	}
}
