// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Engine;
use reifydb_core::interface::Engine as _;
use reifydb_core::{
    Result, Version,
    interface::{CdcEvent, CdcScan, UnversionedTransaction, VersionedTransaction},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

/// A simple flow subsystem that runs in its own thread and periodically polls for CDC events
pub struct FlowSubsystem<VT: VersionedTransaction, UT: UnversionedTransaction> {
    engine: Engine<VT, UT>,
    poll_interval: Duration,
    running: Arc<AtomicBool>,
    last_seen_version: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> FlowSubsystem<VT, UT> {
    /// Create a new flow subsystem with the given engine and poll interval
    pub fn new(engine: Engine<VT, UT>, poll_interval: Duration) -> Self {
        Self {
            engine,
            poll_interval,
            running: Arc::new(AtomicBool::new(false)),
            last_seen_version: Arc::new(AtomicU64::new(0)),
            handle: None,
        }
    }

    /// Stop the flow subsystem
    pub fn stop(&mut self) -> Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already stopped
        }

        self.running.store(false, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            handle.join().expect("Failed to join flow subsystem thread");
        }

        Ok(())
    }

    /// Check if the subsystem is currently running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get the last seen version (for monitoring purposes)
    pub fn last_seen_version(&self) -> Version {
        self.last_seen_version.load(Ordering::Relaxed)
    }
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> FlowSubsystem<VT, UT> {
    /// Start the flow subsystem in a background thread
    pub fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already running
        }

        self.running.store(true, Ordering::Relaxed);

        let engine = self.engine.clone();
        let poll_interval = self.poll_interval;
        let running = Arc::clone(&self.running);
        let last_seen_version = Arc::clone(&self.last_seen_version);

        let handle = thread::spawn(move || {
            println!("[FlowSubsystem] Started CDC event polling with interval {:?}", poll_interval);

            while running.load(Ordering::Relaxed) {
                if let Err(e) = Self::poll_and_print_events(&engine, &last_seen_version) {
                    eprintln!("[FlowSubsystem] Error polling CDC events: {}", e);
                }

                thread::sleep(poll_interval);
            }

            println!("[FlowSubsystem] Stopped CDC event polling");
        });

        self.handle = Some(handle);
        Ok(())
    }

    /// Poll for new CDC events and print them
    fn poll_and_print_events(engine: &Engine<VT, UT>, last_seen_version: &AtomicU64) -> Result<()> {
        // Begin a query transaction to access CDC events
        let mut query_txn = engine.begin_query()?;

        // Use the versioned query transaction to scan CDC events
        // let events: Vec<CdcEvent> =
        //     query_txn.with_versioned_query(|versioned| Ok(CdcScan::scan(versioned)?.collect()))?;
        //
        // let current_last_seen = last_seen_version.load(Ordering::Relaxed);
        // let mut new_events_found = false;
        // let mut max_version_seen = current_last_seen;
        //
        // // Filter and print only new events (versions higher than last seen)
        // for event in events {
        //     if event.version > current_last_seen {
        //         Self::print_cdc_event(&event);
        //         max_version_seen = max_version_seen.max(event.version);
        //         new_events_found = true;
        //     }
        // }
        //
        // // Update the last seen version if we found new events
        // if new_events_found {
        //     last_seen_version.store(max_version_seen, Ordering::Relaxed);
        //     println!("[FlowSubsystem] Updated last seen version to {}", max_version_seen);
        // }
        todo!();

        Ok(())
    }

    /// Format and print a CDC event
    fn print_cdc_event(event: &CdcEvent) {
        let change_description = match &event.change {
            reifydb_core::interface::Change::Insert { key, after } => {
                format!(
                    "INSERT key={:?} value={:?}",
                    String::from_utf8_lossy(&key.0),
                    String::from_utf8_lossy(&after.0)
                )
            }
            reifydb_core::interface::Change::Update { key, before, after } => {
                let before_str = if before.is_deleted() {
                    "<deleted>".to_string()
                } else {
                    format!("{:?}", String::from_utf8_lossy(&before.0))
                };
                format!(
                    "UPDATE key={:?} before={} after={:?}",
                    String::from_utf8_lossy(&key.0),
                    before_str,
                    String::from_utf8_lossy(&after.0)
                )
            }
            reifydb_core::interface::Change::Delete { key, before } => {
                let before_str = if before.is_deleted() {
                    "<deleted>".to_string()
                } else {
                    format!("{:?}", String::from_utf8_lossy(&before.0))
                };
                format!("DELETE key={:?} before={}", String::from_utf8_lossy(&key.0), before_str)
            }
        };

        println!(
            "[CDC] v{} seq{} ts{} | {}",
            event.version, event.sequence, event.timestamp, change_description
        );
    }
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> Drop for FlowSubsystem<VT, UT> {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
