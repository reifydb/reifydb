// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::value::is::IsTemporal;

#[inline]
pub fn is_equal<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l == r
}

#[inline]
pub fn is_not_equal<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l != r
}

#[inline]
pub fn is_greater_than<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l > r
}

#[inline]
pub fn is_greater_than_equal<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l >= r
}

#[inline]
pub fn is_less_than<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l < r
}

#[inline]
pub fn is_less_than_equal<T>(l: &T, r: &T) -> bool
where
	T: IsTemporal,
{
	l <= r
}
