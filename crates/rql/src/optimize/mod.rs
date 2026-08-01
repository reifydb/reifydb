// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Expression-level rewrites over the lowered physical plan. New passes go through `walk_expressions_mut` so they
//! compose into one traversal instead of walking the plan once per pass.

pub mod fold;
pub mod walk;

use crate::plan::physical::PhysicalPlan;

pub fn optimize_physical(plan: &mut PhysicalPlan<'_>) {
	walk::walk_expressions_mut(plan, &mut fold::fold, &mut fold::fold_projection);
}
