// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Garbage collection. Three reclamation strategies cover the cases the multi-version store generates: historical
//! reclaims versions older than the active read watermark; row reclaims tombstones once no reader can see the
//! pre-tombstone version; operator handles per-flow retention overrides where some operators keep less history
//! than the global default.

pub mod historical;
pub mod operator;
pub mod row;
