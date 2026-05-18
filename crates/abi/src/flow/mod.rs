// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for flow change and diff payloads. A flow operator receives `change` events when its inputs have
//! moved and emits `diff` events when its outputs change; both have stable `repr(C)` layouts so the host and
//! guest sides can interpret them identically.

pub mod change;
pub mod diff;
