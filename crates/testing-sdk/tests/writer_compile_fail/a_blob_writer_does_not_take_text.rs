// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::{
	common::extern_c::binding::builder::ColumnsBuilder, error::SdkError, flow::operator::column::sink::ColumnWriter,
};

// Text pushed into a blob column would be stored as bytes and read back as a blob, so it must not compile.
fn emit(builder: &mut ColumnsBuilder<'_>) -> Result<(), SdkError> {
	let mut blob = builder.blob_writer(1, 0)?;
	ColumnWriter::<str>::push(&mut blob, "text")?;
	blob.finish()?;
	Ok(())
}

fn main() {
	let _ = emit;
}
