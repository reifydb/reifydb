// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::{
	error::Result,
	flow::operator::context::{ClassBatch, ClassState, Managed, Unmanaged},
};

// A batch read under one class committed into another class writes keys the second class never owns.
fn commit_across(batch: ClassBatch<Unmanaged, i64>, state: &mut impl ClassState<Managed>) -> Result<()> {
	batch.commit(state)
}

fn main() {}
