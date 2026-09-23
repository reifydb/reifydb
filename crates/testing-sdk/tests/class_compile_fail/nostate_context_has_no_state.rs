// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::flow::operator::context::{GuestContext, Nostate};

fn touch(ctx: &mut impl GuestContext<Nostate>) {
	let _ = ctx.state();
}

fn main() {}
