// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}
