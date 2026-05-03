// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{
			Ast, AstBindingProtocol, AstBindingProtocolKind, AstCreate, AstCreateBinding, AstDrop,
			AstDropBinding, AstLiteral,
		},
		identifier::{MaybeQualifiedBindingIdentifier, MaybeQualifiedProcedureIdentifier},
		parse::{Parser, Precedence},
	},
	bump::BumpFragment,
	diagnostic::AstError,
	token::{
		keyword::Keyword,
		operator::Operator,
		separator::Separator,
		token::{Token, TokenKind},
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_create_binding(
		&mut self,
		token: Token<'bump>,
		kind: AstBindingProtocolKind,
	) -> Result<AstCreate<'bump>> {
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let binding_ident = MaybeQualifiedBindingIdentifier::new(name).with_namespace(namespace);

		self.consume_keyword(Keyword::For)?;
		let mut proc_segments = self.parse_double_colon_separated_identifiers()?;
		let proc_name = proc_segments.pop().unwrap().into_fragment();
		let proc_namespace: Vec<_> = proc_segments.into_iter().map(|s| s.into_fragment()).collect();
		let procedure = MaybeQualifiedProcedureIdentifier::new(proc_name).with_namespace(proc_namespace);

		self.consume_keyword(Keyword::With)?;
		let pairs = parse_with_block(self)?;

		let mut method = None;
		let mut path = None;
		let mut rpc_name = None;
		let mut format = None;
		for (key, value) in pairs {
			match key.text() {
				"method" => method = Some(value),
				"path" => path = Some(value),
				"name" => rpc_name = Some(value),
				"format" => format = Some(value),
				_ => {
					let expected = match kind {
						AstBindingProtocolKind::Http => "one of: method, path, format",
						AstBindingProtocolKind::Grpc | AstBindingProtocolKind::Ws => {
							"one of: name, format"
						}
					};
					return Err(AstError::UnexpectedToken {
						expected: expected.to_string(),
						fragment: key.to_owned(),
					}
					.into());
				}
			}
		}

		let protocol = AstBindingProtocol {
			kind,
			method,
			path,
			rpc_name,
			format,
		};

		Ok(AstCreate::Binding(AstCreateBinding {
			token,
			name: binding_ident,
			procedure,
			protocol,
		}))
	}

	pub(crate) fn parse_drop_binding(&mut self, token: Token<'bump>) -> Result<AstDrop<'bump>> {
		let if_exists = self.consume_if(TokenKind::Keyword(Keyword::If))?.is_some();
		if if_exists {
			self.consume_keyword(Keyword::Exists)?;
		}

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		let name = segments.pop().unwrap().into_fragment();
		let namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let binding = MaybeQualifiedBindingIdentifier::new(name).with_namespace(namespace);

		Ok(AstDrop::Binding(AstDropBinding {
			token,
			if_exists,
			binding,
		}))
	}
}

fn parse_with_block<'bump>(parser: &mut Parser<'bump>) -> Result<Vec<(BumpFragment<'bump>, BumpFragment<'bump>)>> {
	parser.consume_operator(Operator::OpenCurly)?;
	let mut pairs = Vec::new();

	loop {
		parser.skip_new_line()?;

		if parser.current()?.is_operator(Operator::CloseCurly) {
			break;
		}

		let key = parser.consume(TokenKind::Identifier)?.fragment;
		parser.consume_operator(Operator::Colon)?;

		let value_node = parser.parse_node(Precedence::None)?;
		let value = extract_text_literal(&value_node)?;

		pairs.push((key, value));

		parser.skip_new_line()?;

		if parser.consume_if(TokenKind::Separator(Separator::Comma))?.is_some() {
			continue;
		}

		if parser.current()?.is_operator(Operator::CloseCurly) {
			break;
		}
	}

	parser.consume_operator(Operator::CloseCurly)?;
	Ok(pairs)
}

fn extract_text_literal<'bump>(node: &Ast<'bump>) -> Result<BumpFragment<'bump>> {
	match node {
		Ast::Literal(AstLiteral::Text(text)) => Ok(text.0.fragment),
		other => Err(AstError::UnexpectedToken {
			expected: "string literal".to_string(),
			fragment: other.token().fragment.to_owned(),
		}
		.into()),
	}
}
