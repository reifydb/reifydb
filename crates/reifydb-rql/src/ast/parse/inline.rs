// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::ast::{
	AstInline, AstInlineKeyedValue, TokenKind,
	parse::{Parser, Precedence},
	tokenize::{
		Operator,
		Operator::{CloseCurly, Colon},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_inline(&mut self) -> crate::Result<AstInline<'a>> {
		let token = self.consume_operator(Operator::OpenCurly)?;

		let mut keyed_values = Vec::with_capacity(4);
		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			let key = self.parse_as_identifier()?;
			self.consume_operator(Colon)?;
			let value =
				Box::new(self.parse_node(Precedence::None)?);

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
mod tests {
	use crate::ast::{
		Ast::{Identifier, Literal},
		AstLiteral::{Number, Text},
		parse::parse,
		tokenize::tokenize,
	};

	#[test]
	fn test_empty_inline() {
		let tokens = tokenize("{}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 0);
	}

	#[test]
	fn test_single_keyed_value() {
		let tokens = tokenize("{id: 1}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.value(), "id");
		let Literal(Number(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "1");
	}

	#[test]
	fn test_keyword() {
		let tokens = tokenize("{value: 1}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.value(), "value");
		let Literal(Number(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "1");
	}

	#[test]
	fn test_text() {
		let tokens = tokenize(r#"{text: 'Ada'}"#).unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.value(), "text");
		let Literal(Text(value)) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(value.value(), "Ada");
	}

	#[test]
	fn test_multiple_keyed_values() {
		let tokens = tokenize(r#"{id: 1, name: 'Ada'}"#).unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.value(), "id");
		let Literal(Number(value)) = id_keyed_value.value.as_ref()
		else {
			panic!()
		};
		assert_eq!(value.value(), "1");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.value(), "name");
		let Literal(Text(value)) = name_keyed_value.value.as_ref()
		else {
			panic!()
		};
		assert_eq!(value.value(), "Ada");
	}

	#[test]
	fn test_identifier_value() {
		let tokens = tokenize("{keyed_value: someVariable}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let keyed_value = &inline[0];
		assert_eq!(keyed_value.key.value(), "keyed_value");
		let Identifier(identifier) = keyed_value.value.as_ref() else {
			panic!()
		};
		assert_eq!(identifier.value(), "someVariable");
	}

	#[test]
	fn test_multiline_inline() {
		let tokens = tokenize(
			r#"{
            id: 42,
            name: 'Database',
            active: true
        }"#,
		)
		.unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 3);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.value(), "id");
		let Literal(Number(value)) = id_keyed_value.value.as_ref()
		else {
			panic!()
		};
		assert_eq!(value.value(), "42");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.value(), "name");
		let Literal(Text(value)) = name_keyed_value.value.as_ref()
		else {
			panic!()
		};
		assert_eq!(value.value(), "Database");

		let active_keyed_value = &inline[2];
		assert_eq!(active_keyed_value.key.value(), "active");
		assert!(active_keyed_value.value.is_literal_boolean());
	}

	#[test]
	fn test_trailing_comma() {
		let tokens = tokenize("{id: 1, name: 'Test',}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let id_keyed_value = &inline[0];
		assert_eq!(id_keyed_value.key.value(), "id");

		let name_keyed_value = &inline[1];
		assert_eq!(name_keyed_value.key.value(), "name");
	}

	#[test]
	fn test_comptokenize_values() {
		let tokens =
			tokenize("{result: (1 + 2), enabled: !false}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 2);

		let result_keyed_value = &inline[0];
		assert_eq!(result_keyed_value.key.value(), "result");
		assert!(result_keyed_value.value.is_tuple());

		let enabled_keyed_value = &inline[1];
		assert_eq!(enabled_keyed_value.key.value(), "enabled");
		assert!(enabled_keyed_value.value.is_prefix());
	}

	#[test]
	fn test_nested_inline() {
		let tokens = tokenize("{user: {id: 1, name: 'John'}}").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let inline = result[0].first_unchecked().as_block();
		assert_eq!(inline.len(), 1);

		let user_keyed_value = &inline[0];
		assert_eq!(user_keyed_value.key.value(), "user");
		assert!(user_keyed_value.value.is_block());

		let nested_inline = user_keyed_value.value.as_block();
		assert_eq!(nested_inline.len(), 2);
		assert_eq!(nested_inline[0].key.value(), "id");
		assert_eq!(nested_inline[1].key.value(), "name");
	}
}
