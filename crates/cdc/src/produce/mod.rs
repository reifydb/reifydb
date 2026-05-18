// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Producer side of the CDC stream. The transaction layer hands committed deltas here at commit time; the producer
//! encodes them into CDC records, persists them to storage, and advances the publisher watermark so consumers can
//! observe the new commit boundary. Decoding lives next to producing so the round-trip is symmetric.

pub(crate) mod decode;
pub mod producer;
pub mod watermark;
