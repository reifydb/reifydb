// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::rule::{Function, SignatureOwner};

const LIMIT: usize = 4;

pub(crate) fn breaks(function: &Function) -> bool {
	if breaks_is_exempt(function) {
		return false;
	}
	function.signature.inputs.len() > LIMIT
}

fn breaks_is_exempt(function: &Function) -> bool {
	if function.signature_owner == SignatureOwner::Trait {
		return true;
	}
	let Some(abi) = function.abi else {
		return false;
	};
	match &abi.name {
		Some(name) => name.value() == "C",
		None => true,
	}
}

#[cfg(test)]
mod tests;
