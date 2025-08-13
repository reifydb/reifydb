// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}
