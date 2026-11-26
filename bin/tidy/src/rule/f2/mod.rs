// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::Function;

const LIMIT: usize = 60;

pub(crate) fn breaks(function: &Function) -> bool {
	let Some(body) = function.body else {
		return false;
	};
	let span = body.brace_token.span;
	let opened = span.open().start().line;
	let closed = span.close().start().line;
	closed.saturating_sub(opened).saturating_sub(1) > LIMIT
}

#[cfg(test)]
mod tests;
