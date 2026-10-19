// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef as ArrowArrayRef};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::value_type::{ValueType, field::from_field},
};
use vortex_array::{
	ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute, aggregate_fn::fns::is_constant::is_constant,
	arrays::ConstantArray,
};
#[cfg(feature = "pco")]
use vortex_btrblocks::schemes::{float::PcoScheme as FloatPcoScheme, integer::PcoScheme as IntegerPcoScheme};
use vortex_btrblocks::{BtrBlocksCompressor, BtrBlocksCompressorBuilder};
use vortex_session::VortexSession;

use crate::{block::ColumnChunks, convert::to_vortex, error::vortex};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompressionProfile {
	#[default]
	Default,
	#[cfg(feature = "pco")]
	Compact,
}

#[derive(Clone)]
pub struct Compressor {
	session: VortexSession,
	compressor: BtrBlocksCompressor,
}

impl Compressor {
	pub fn new(session: VortexSession) -> Self {
		Self::with_profile(session, CompressionProfile::Default)
	}

	pub fn with_profile(session: VortexSession, profile: CompressionProfile) -> Self {
		let builder = BtrBlocksCompressorBuilder::default();
		let builder = match profile {
			CompressionProfile::Default => builder,
			#[cfg(feature = "pco")]
			CompressionProfile::Compact => builder.with_new_scheme(&IntegerPcoScheme).with_new_scheme(&FloatPcoScheme),
		};
		Self {
			session,
			compressor: builder.build(),
		}
	}

	pub fn compress(&self, ty: ValueType, column: &(FieldRef, ArrowArrayRef)) -> Result<ColumnChunks> {
		let field_type = from_field(&column.0)?;
		let imported = to_vortex(&self.session, column)?;
		let mut ctx = self.session.create_execution_ctx();
		let compressed = match constant(&imported, column.1.null_count(), &mut ctx)? {
			Some(folded) => folded,
			None => self.compressor.compress(&imported, &mut ctx).map_err(vortex("compress"))?,
		};
		Ok(ColumnChunks::single(ty, column.0.is_nullable(), field_type, compressed))
	}
}

fn constant(array: &ArrayRef, nones: usize, ctx: &mut ExecutionCtx) -> Result<Option<ArrayRef>> {
	if array.len() < 2 || nones > 0 || !is_constant(array, ctx).map_err(vortex("constant"))? {
		return Ok(None);
	}
	let value = array.execute_scalar(0, ctx).map_err(vortex("constant"))?;
	Ok(Some(ConstantArray::new(value, array.len()).into_array()))
}
