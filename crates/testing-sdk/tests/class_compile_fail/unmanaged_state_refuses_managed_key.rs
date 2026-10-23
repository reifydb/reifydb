// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::state::ManagedKey;
use reifydb_sdk::{
	error::Result,
	flow::operator::context::{ClassState, GuestContext, Unmanaged},
};

fn read(ctx: &mut impl GuestContext<Unmanaged>, key: &ManagedKey) -> Result<Vec<Option<i64>>> {
	ctx.state().get::<i64>(std::slice::from_ref(key))
}

fn main() {}
