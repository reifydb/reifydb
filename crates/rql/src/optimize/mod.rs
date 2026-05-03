// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod fold;
pub mod walk;

use crate::plan::physical::PhysicalPlan;

pub fn optimize_physical(plan: &mut PhysicalPlan<'_>) {
	walk::walk_expressions_mut(plan, &mut fold::fold, &mut fold::fold_projection);
}
