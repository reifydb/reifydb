// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(feature = "embedded_async")]
pub mod embedded_async;

#[cfg(feature = "embedded_sync")]
pub mod embedded_sync;

#[cfg(feature = "server")]
pub mod server;
