// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "parity/common.rs"]
mod common;

#[path = "parity/take.rs"]
mod take;

#[path = "parity/distinct.rs"]
mod distinct;

#[path = "parity/window.rs"]
mod window;

#[path = "parity/aggregate.rs"]
mod aggregate;

#[path = "parity/join.rs"]
mod join;

#[path = "parity/sort.rs"]
mod sort;

#[path = "parity/filter.rs"]
mod filter;

#[path = "parity/map.rs"]
mod map;

#[path = "parity/gate.rs"]
mod gate;

#[path = "parity/extend.rs"]
mod extend;

#[path = "parity/embedded_hydration.rs"]
mod embedded_hydration;

#[path = "parity/policy_scope.rs"]
mod policy_scope;

#[path = "parity/volume.rs"]
mod volume;
