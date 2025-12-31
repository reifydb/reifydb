// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::cmp::Ordering;

use crate::value::{is::IsNumber, number::Promote};

#[inline]
pub fn partial_cmp<L, R>(l: &L, r: &R) -> Option<Ordering>
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	l.checked_promote(r).and_then(|(lp, rp)| lp.partial_cmp(&rp))
}

#[inline]
pub fn is_equal<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_some_and(|o| o == Ordering::Equal)
}

#[inline]
pub fn is_not_equal<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_none_or(|o| o != Ordering::Equal)
}

#[inline]
pub fn is_greater_than<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_some_and(|o| o == Ordering::Greater)
}

#[inline]
pub fn is_greater_than_equal<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_some_and(|o| o != Ordering::Less)
}

#[inline]
pub fn is_less_than<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_some_and(|o| o == Ordering::Less)
}

#[inline]
pub fn is_less_than_equal<L, R>(l: &L, r: &R) -> bool
where
	L: Promote<R>,
	R: IsNumber,
	<L as Promote<R>>::Output: IsNumber,
{
	partial_cmp(l, r).is_some_and(|o| o != Ordering::Greater)
}
