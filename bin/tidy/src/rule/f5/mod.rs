// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{
	FnArg::{self, Typed},
	Type::Path,
};

use crate::rule::{Function, SignatureOwner};

pub(crate) fn breaks(function: &Function) -> bool {
	if function.signature_owner == SignatureOwner::Trait {
		return false;
	}
	function.signature.inputs.iter().any(breaks_is_bool)
}

fn breaks_is_bool(argument: &FnArg) -> bool {
	let Typed(typed) = argument else {
		return false;
	};
	let Path(path) = typed.ty.as_ref() else {
		return false;
	};
	path.qself.is_none() && path.path.is_ident("bool")
}

#[cfg(test)]
mod tests;
