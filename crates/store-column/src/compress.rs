// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::value_type::{ValueType, field::from_field},
};
use vortex_array::VortexSessionExecute;
#[cfg(feature = "pco")]
use vortex_btrblocks::schemes::{float::PcoScheme as FloatPcoScheme, integer::PcoScheme as IntegerPcoScheme};
use vortex_btrblocks::{BtrBlocksCompressor, BtrBlocksCompressorBuilder};
use vortex_session::VortexSession;

use crate::{convert::to_vortex, error::vortex, snapshot::ColumnChunks};

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
		let compressed = self.compressor.compress(&imported, &mut ctx).map_err(vortex("compress"))?;
		Ok(ColumnChunks::single(ty, column.0.is_nullable(), field_type, compressed))
	}
}
