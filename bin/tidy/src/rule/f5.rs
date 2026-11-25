// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use syn::{FnArg, Type};

use crate::rule::Function;

pub fn breaks(function: &Function) -> bool {
	function.signature.inputs.iter().any(breaks_is_bool)
}

fn breaks_is_bool(argument: &FnArg) -> bool {
	let FnArg::Typed(typed) = argument else {
		return false;
	};
	let Type::Path(path) = typed.ty.as_ref() else {
		return false;
	};
	path.qself.is_none() && path.path.is_ident("bool")
}
