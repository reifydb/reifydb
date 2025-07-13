// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::num::is::IsNumber;
use crate::num::promote::Promote;

pub fn is_equal<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp == rp).unwrap_or(false)
}

pub fn is_not_equal<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp != rp).unwrap_or(true)
}

pub fn is_greater_than<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp > rp).unwrap_or(false)
}

pub fn is_greater_than_equal<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp >= rp).unwrap_or(false)
}

pub fn is_less_than<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp < rp).unwrap_or(false)
}

pub fn is_less_than_equal<L, R>(l: L, r: R) -> bool
where
    L: Promote<R>,
    R: IsNumber,
    <L as Promote<R>>::Output: IsNumber,
{
    l.checked_promote(r).map(|(lp, rp)| lp <= rp).unwrap_or(false)
}
