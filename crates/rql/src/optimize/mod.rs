// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Physical-plan post-passes. Walks the lowered plan and applies expression-level rewrites - constant folding,
//! projection simplification - that were not worth doing during logical compilation. New optimisations register
//! through the same `walk_expressions_mut` interface so they can be composed without re-traversing the plan once
//! per pass.

pub mod fold;
pub mod walk;

use crate::plan::physical::PhysicalPlan;

pub fn optimize_physical(plan: &mut PhysicalPlan<'_>) {
	walk::walk_expressions_mut(plan, &mut fold::fold, &mut fold::fold_projection);
}
