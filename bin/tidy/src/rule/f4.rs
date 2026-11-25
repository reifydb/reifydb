// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::Function;

const LIMIT: usize = 4;

pub fn breaks(function: &Function) -> bool {
	if breaks_exempt(function) {
		return false;
	}
	function.signature.inputs.len() > LIMIT
}

fn breaks_exempt(function: &Function) -> bool {
	let Some(abi) = function.abi else {
		return false;
	};
	match &abi.name {
		Some(name) => name.value() == "C",
		None => true,
	}
}
