// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::{CommandSession, QuerySession, SessionConfig, SessionPreset};
use crate::session::config::{ByteSize, IsolationLevel};
use reifydb_core::interface::{Engine, Principal, UnversionedTransaction, VersionedTransaction};
use std::time::Duration;

pub struct QuerySessionBuilder<'a, VT, UT> {
    engine: &'a dyn Engine<VT, UT>,
    principal: Principal,
    config: SessionConfig,
}

impl<'a, VT, UT> QuerySessionBuilder<'a, VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: &'a dyn Engine<VT, UT>, principal: Principal) -> Self {
        Self {
            engine,
            principal,
            config: SessionConfig::default(),
        }
    }
    
    pub fn with_preset(mut self, preset: SessionPreset) -> Self {
        self.config = preset.config();
        self
    }
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }
    
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.config.max_rows = Some(max_rows);
        self
    }
    
    pub fn with_max_memory(mut self, max_memory: ByteSize) -> Self {
        self.config.max_memory = Some(max_memory);
        self
    }
    
    pub fn with_compute_budget(mut self, units: u64) -> Self {
        self.config.max_compute_units = Some(units);
        self
    }
    
    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }
    
    pub fn allow_full_scans(mut self, allow: bool) -> Self {
        self.config.allow_full_scans = allow;
        self
    }
    
    pub fn build(self) -> QuerySession<'a, VT, UT> {
        QuerySession::new(self.engine, self.principal, self.config)
    }
}

pub struct CommandSessionBuilder<'a, VT, UT> {
    engine: &'a dyn Engine<VT, UT>,
    principal: Principal,
    config: SessionConfig,
}

impl<'a, VT, UT> CommandSessionBuilder<'a, VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: &'a dyn Engine<VT, UT>, principal: Principal) -> Self {
        Self {
            engine,
            principal,
            config: SessionConfig::default(),
        }
    }
    
    pub fn with_preset(mut self, preset: SessionPreset) -> Self {
        self.config = preset.config();
        self
    }
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }
    
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.config.max_rows = Some(max_rows);
        self
    }
    
    pub fn with_max_memory(mut self, max_memory: ByteSize) -> Self {
        self.config.max_memory = Some(max_memory);
        self
    }
    
    pub fn with_compute_budget(mut self, units: u64) -> Self {
        self.config.max_compute_units = Some(units);
        self
    }
    
    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }
    
    pub fn allow_full_scans(mut self, allow: bool) -> Self {
        self.config.allow_full_scans = allow;
        self
    }
    
    pub fn build(self) -> CommandSession<'a, VT, UT> {
        CommandSession::new(self.engine, self.principal, self.config)
    }
}