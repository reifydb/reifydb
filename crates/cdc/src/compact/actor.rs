// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Periodic CDC compaction actor. Ticks at `CdcCompactInterval`, packs
//! `CdcCompactMaxBlocksPerTick` blocks per tick into the `cdc_block` table.
//! All knobs are read fresh from system config every tick so `SET CONFIG`
//! takes effect within one window without restart.

use std::{sync::Arc, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	traits::{Actor, Directive},
};
use tracing::{debug, error, trace};

use crate::{produce::watermark::CdcProducerWatermark, storage::sqlite::storage::SqliteCdcStorage};

pub enum CompactMessage {
	/// Periodic compaction: pack up to `CdcCompactMaxBlocksPerTick` blocks
	/// respecting `CdcCompactSafetyLag`. Reschedules itself.
	Tick,
	/// Drain everything compactable, ignoring safety lag and allowing a
	/// partial final block. Admin/test trigger. Fire-and-forget.
	CompactAll,
}

pub struct CompactActor {
	config: Arc<dyn GetConfig>,
	store: SqliteCdcStorage,
	watermark: CdcProducerWatermark,
}

impl CompactActor {
	pub fn new(config: Arc<dyn GetConfig>, store: SqliteCdcStorage, watermark: CdcProducerWatermark) -> Self {
		Self {
			config,
			store,
			watermark,
		}
	}

	fn read_block_size(&self) -> usize {
		self.config.get_config_uint8(ConfigKey::CdcCompactBlockSize) as usize
	}

	fn read_safety_lag(&self) -> u64 {
		self.config.get_config_uint8(ConfigKey::CdcCompactSafetyLag)
	}

	fn read_max_blocks_per_tick(&self) -> usize {
		self.config.get_config_uint8(ConfigKey::CdcCompactMaxBlocksPerTick) as usize
	}

	fn read_interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::CdcCompactInterval)
	}

	fn read_zstd_level(&self) -> u8 {
		self.config.get_config_uint1(ConfigKey::CdcCompactZstdLevel)
	}
}

impl Actor for CompactActor {
	type State = ();
	type Message = CompactMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		let interval = self.read_interval();
		debug!("[CdcCompact] started: interval={:?}", interval);
		ctx.schedule_once(interval, || CompactMessage::Tick);
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			debug!("[CdcCompact] stopped");
			return Directive::Stop;
		}
		match msg {
			CompactMessage::Tick => self.on_tick(ctx),
			CompactMessage::CompactAll => self.on_compact_all(),
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(8)
	}
}

impl CompactActor {
	#[inline]
	fn on_tick(&self, ctx: &Context<CompactMessage>) {
		let block_size = self.read_block_size();
		let safety_lag = self.read_safety_lag();
		let max_blocks = self.read_max_blocks_per_tick();
		let zstd_level = self.read_zstd_level();
		let watermark = self.watermark.get();

		let produced = self.run_tick_loop(block_size, safety_lag, zstd_level, watermark, max_blocks);
		if produced > 0 {
			debug!("[CdcCompact] produced {produced} block(s) this tick");
		}

		ctx.schedule_once(self.read_interval(), || CompactMessage::Tick);
	}

	#[inline]
	fn run_tick_loop(
		&self,
		block_size: usize,
		safety_lag: u64,
		zstd_level: u8,
		watermark: CommitVersion,
		max_blocks: usize,
	) -> usize {
		let mut produced = 0usize;
		while produced < max_blocks {
			match self.store.compact_oldest(block_size, safety_lag, zstd_level, watermark) {
				Ok(Some(s)) => {
					trace!(
						"[CdcCompact] block: [{}..{}] entries={} bytes={}",
						s.min_version.0, s.max_version.0, s.num_entries, s.compressed_bytes,
					);
					produced += 1;
				}
				Ok(None) => break,
				Err(e) => {
					error!("[CdcCompact] {e}");
					break;
				}
			}
		}
		produced
	}

	#[inline]
	fn on_compact_all(&self) {
		let block_size = self.read_block_size();
		let zstd_level = self.read_zstd_level();
		let watermark = self.watermark.get();
		match self.store.compact_all(block_size, zstd_level, watermark) {
			Ok(s) => debug!("[CdcCompact] CompactAll produced {} block(s)", s.len()),
			Err(e) => error!("[CdcCompact] CompactAll error: {e}"),
		}
	}
}
