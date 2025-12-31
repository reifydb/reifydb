// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operator-specific compilation implementations

mod aggregate;
mod apply;
mod distinct;
mod extend;
mod filter;
mod join;
mod map;
mod merge;
mod sort;
mod take;
mod window;

pub(crate) use aggregate::AggregateCompiler;
pub(crate) use apply::ApplyCompiler;
pub(crate) use distinct::DistinctCompiler;
pub(crate) use extend::ExtendCompiler;
pub(crate) use filter::FilterCompiler;
pub(crate) use join::JoinCompiler;
pub(crate) use map::MapCompiler;
pub(crate) use merge::MergeCompiler;
pub(crate) use sort::SortCompiler;
pub(crate) use take::TakeCompiler;
pub(crate) use window::WindowCompiler;
