// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[path = "subscription/common.rs"]
mod common;

#[path = "subscription/take.rs"]
mod take;

#[path = "subscription/distinct.rs"]
mod distinct;

#[path = "subscription/window.rs"]
mod window;

#[path = "subscription/aggregate.rs"]
mod aggregate;

#[path = "subscription/join.rs"]
mod join;

#[path = "subscription/sort.rs"]
mod sort;

#[path = "subscription/filter.rs"]
mod filter;

#[path = "subscription/map.rs"]
mod map;

#[path = "subscription/gate.rs"]
mod gate;

#[path = "subscription/extend.rs"]
mod extend;
