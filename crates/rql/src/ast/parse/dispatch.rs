// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::AstDispatch,
		identifier::MaybeQualifiedSumTypeIdentifier,
		parse::{Parser, Precedence},
	},
	bump::BumpBox,
	diagnostic::AstError,
	token::{keyword::Keyword, operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_dispatch(&mut self) -> Result<AstDispatch<'bump>> {
		let token = self.consume_keyword(Keyword::Dispatch)?;

		let mut segments = self.parse_double_colon_separated_identifiers()?;
		if segments.len() < 2 {
			return Err(AstError::UnexpectedToken {
				expected: "qualified event type (e.g. EventType::Variant)".to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}

		let variant = segments.pop().unwrap().into_fragment();

		let event_name_frag = segments.pop().unwrap().into_fragment();

		let event_namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let on_event = if event_namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag).with_namespace(event_namespace)
		};

		self.consume_operator(Operator::OpenCurly)?;
		let mut fields = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let field_name = self.parse_identifier_with_hyphens()?.into_fragment();
			self.consume_operator(Operator::Colon)?;
			let value = BumpBox::new_in(self.parse_node(Precedence::None)?, self.bump());

			fields.push((field_name, value));

			self.skip_new_line()?;
			if self.consume_if(TokenKind::Separator(Separator::Comma))?.is_none() {
				self.skip_new_line()?;
				break;
			}
		}

		self.consume_operator(Operator::CloseCurly)?;

		Ok(AstDispatch {
			token,
			on_event,
			variant,
			fields,
		})
	}
}
