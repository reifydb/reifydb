// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::state::UnmanagedKey;
use reifydb_sdk::{
	error::Result,
	flow::operator::context::{ClassState, GuestContext, Managed},
};

fn read(ctx: &mut impl GuestContext<Managed>, key: &UnmanagedKey) -> Result<Option<i64>> {
	ctx.state().get::<i64>(key)
}

fn main() {}
