// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub max_rows: Option<usize>,
    pub timeout: Option<Duration>,
    pub max_memory: Option<ByteSize>,
    pub max_compute_units: Option<u64>,
    pub isolation_level: IsolationLevel,
    pub allow_full_scans: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_rows: None,
            timeout: None,
            max_memory: None,
            max_compute_units: None,
            isolation_level: IsolationLevel::SnapshotIsolation,
            allow_full_scans: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SessionPreset {
    Interactive,
    Standard,
    Batch,
}

impl SessionPreset {
    pub fn config(&self) -> SessionConfig {
        match self {
            SessionPreset::Interactive => SessionConfig {
                max_rows: Some(1_000),
                timeout: Some(Duration::from_secs(5)),
                max_memory: Some(ByteSize::mb(100)),
                max_compute_units: Some(100),
                isolation_level: IsolationLevel::SnapshotIsolation,
                allow_full_scans: false,
            },
            SessionPreset::Standard => SessionConfig {
                max_rows: Some(10_000),
                timeout: Some(Duration::from_secs(30)),
                max_memory: Some(ByteSize::gb(1)),
                max_compute_units: Some(1_000),
                isolation_level: IsolationLevel::SnapshotIsolation,
                allow_full_scans: true,
            },
            SessionPreset::Batch => SessionConfig {
                max_rows: None,
                timeout: Some(Duration::from_secs(300)),
                max_memory: Some(ByteSize::gb(10)),
                max_compute_units: Some(10_000),
                isolation_level: IsolationLevel::Serializable,
                allow_full_scans: true,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    SnapshotIsolation,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn bytes(n: u64) -> Self {
        Self(n)
    }
    
    pub const fn kb(n: u64) -> Self {
        Self(n * 1024)
    }
    
    pub const fn mb(n: u64) -> Self {
        Self(n * 1024 * 1024)
    }
    
    pub const fn gb(n: u64) -> Self {
        Self(n * 1024 * 1024 * 1024)
    }
    
    pub fn as_bytes(&self) -> u64 {
        self.0
    }
}