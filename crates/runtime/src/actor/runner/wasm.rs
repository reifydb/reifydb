// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM actor runner (empty).
//!
//! On WASM, actors process messages inline (synchronously) when sent,
//! so no separate runner is needed. The processing logic is handled
//! directly in the `ActorRuntime::spawn_inner` method.
