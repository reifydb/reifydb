// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}
