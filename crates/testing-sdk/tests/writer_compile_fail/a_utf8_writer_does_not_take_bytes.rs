// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_sdk::{
	common::extern_c::binding::builder::ColumnsBuilder, error::SdkError, flow::operator::column::sink::ColumnWriter,
};

// Bytes pushed into a text column skip the utf8 check and reach the host as an invalid string.
fn emit(builder: &mut ColumnsBuilder<'_>) -> Result<(), SdkError> {
	let mut text = builder.utf8_writer(1, 0)?;
	ColumnWriter::<[u8]>::push(&mut text, b"bytes".as_slice())?;
	text.finish()?;
	Ok(())
}

fn main() {
	let _ = emit;
}
