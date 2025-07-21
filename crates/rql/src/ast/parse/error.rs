// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::{Token, TokenKind};
use reifydb_core::error::diagnostic::{Diagnostic, ast};

pub(crate) fn expected_identifier_error(got: Token) -> Diagnostic {
    ast::expected_identifier_error(got.span)
}

pub(crate) fn invalid_policy_error(got: Token) -> Diagnostic {
    ast::invalid_policy_error(got.span)
}

pub(crate) fn unexpected_token_error(expected: TokenKind, got: Token) -> Diagnostic {
    ast::unexpected_token_error(&format!("{:?}", expected), got.span)
}

pub(crate) fn unsupported_token_error(got: Token) -> Diagnostic {
    ast::unsupported_token_error(got.span)
}
