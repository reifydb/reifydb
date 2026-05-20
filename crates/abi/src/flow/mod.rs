// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for flow change and diff payloads. A flow operator receives `change` events when its inputs have
//! moved and emits `diff` events when its outputs change; both have stable `repr(C)` layouts so the host and
//! guest sides can interpret them identically.

pub mod change;
pub mod diff;
