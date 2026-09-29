// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::value_type::{ValueType, field::from_field},
};
use vortex_array::VortexSessionExecute;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_session::VortexSession;

use crate::{convert::to_vortex, error::vortex, snapshot::ColumnChunks};

#[derive(Clone)]
pub struct Compressor {
	session: VortexSession,
}

impl Compressor {
	pub fn new(session: VortexSession) -> Self {
		Self {
			session,
		}
	}

	pub fn compress(&self, ty: ValueType, column: &(FieldRef, ArrowArrayRef)) -> Result<ColumnChunks> {
		let field_type = from_field(&column.0)?;
		let imported = to_vortex(&self.session, column)?;
		let mut ctx = self.session.create_execution_ctx();
		let compressed = BtrBlocksCompressorBuilder::default()
			.build()
			.compress(&imported, &mut ctx)
			.map_err(vortex("compress"))?;
		Ok(ColumnChunks::single(ty, column.0.is_nullable(), field_type, compressed))
	}
}
