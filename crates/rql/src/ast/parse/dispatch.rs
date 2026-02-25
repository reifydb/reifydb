// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{ast::AstDispatch, identifier::MaybeQualifiedSumTypeIdentifier, parse::Parser},
	bump::BumpBox,
	token::{operator::Operator, separator::Separator, token::TokenKind},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_dispatch(&mut self) -> crate::Result<AstDispatch<'bump>> {
		let token = self.consume_keyword(crate::token::keyword::Keyword::Dispatch)?;

		// Parse event type and variant: ns::EventType::Variant (all :: separated)
		let mut segments = self.parse_double_colon_separated_identifiers()?;
		// Last segment is the variant name
		let variant = segments.pop().unwrap().into_fragment();
		// Second-to-last is the event type name
		let event_name_frag = segments.pop().unwrap().into_fragment();
		// Remaining segments are the namespace
		let event_namespace: Vec<_> = segments.into_iter().map(|s| s.into_fragment()).collect();
		let on_event = if event_namespace.is_empty() {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag)
		} else {
			MaybeQualifiedSumTypeIdentifier::new(event_name_frag).with_namespace(event_namespace)
		};

		// Parse field payload: { field: expr, ... }
		self.consume_operator(Operator::OpenCurly)?;
		let mut fields = Vec::new();

		loop {
			self.skip_new_line()?;

			if self.current()?.is_operator(Operator::CloseCurly) {
				break;
			}

			let field_name = self.parse_identifier_with_hyphens()?.into_fragment();
			self.consume_operator(Operator::Colon)?;
			let value = BumpBox::new_in(self.parse_node(crate::ast::parse::Precedence::None)?, self.bump());

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
