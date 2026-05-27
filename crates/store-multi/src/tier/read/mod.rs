// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the multi-version store. Serves cold keys that the commit buffer has already evicted below
//! the eviction watermark, so a repeated point read does not have to fall through to the persistent tier every
//! time. Only the latest committed `(version, value)` per key is cached; a hit is served only when the requested
//! snapshot version is at or above the cached version, otherwise the caller reads through to the persistent tier
//! which honors the full version bound. Range scans never consult this tier - a partial cache cannot answer a range
//! correctly.

pub mod buffer;
