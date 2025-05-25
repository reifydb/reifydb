// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub enum CowVec<T>
where
    T: Clone,
{
    Owned(Vec<T>),
    Shared(Rc<Vec<T>>),
}

impl<T> IntoIterator for CowVec<T>
where
    T: Clone,
{
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            CowVec::Owned(v) => v.into_iter(),
            CowVec::Shared(rc) => rc.as_ref().clone().into_iter(),
        }
    }
}

impl<T> CowVec<T>
where
    T: Clone,
{
    pub fn new(vec: Vec<T>) -> Self {
        CowVec::Owned(vec)
    }

    pub fn from_shared(rc: Rc<Vec<T>>) -> Self {
        CowVec::Shared(rc)
    }

    /// Ensures unique ownership and returns a mutable reference to the inner Vec.
    pub fn make_mut(&mut self) -> &mut Vec<T> {
        match self {
            CowVec::Owned(vec) => vec,
            CowVec::Shared(rc) => {
                let cloned = (**rc).clone();
                *self = CowVec::Owned(cloned);
                match self {
                    CowVec::Owned(vec) => vec,
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Access as immutable slice.
    pub fn as_slice(&self) -> &[T] {
        match self {
            CowVec::Owned(vec) => vec,
            CowVec::Shared(rc) => rc,
        }
    }

    /// Get by index.
    pub fn get(&self, idx: usize) -> Option<&T> {
        self.as_slice().get(idx)
    }

    pub fn reorder(&mut self, indices: &[usize]) {
        let vec = self.make_mut();

        let len = vec.len();
        assert_eq!(len, indices.len());

        let mut visited = vec![false; len];

        for start in 0..len {
            if visited[start] || indices[start] == start {
                continue;
            }

            let mut current = start;

            while !visited[current] {
                visited[current] = true;
                let next = indices[current];

                if next == start {
                    break;
                }

                vec.swap(current, next);
                current = next;
            }
        }
    }

    /// Set by index (copy-on-write if necessary).
    pub fn set(&mut self, idx: usize, value: T) {
        let vec = self.make_mut();
        vec[idx] = value;
    }

    pub fn push(&mut self, value: T) {
        let vec = self.make_mut();
        vec.push(value);
    }

    /// Extends (copy-on-write if necessary).
    pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) {
        let vec = self.make_mut();
        vec.extend(iter);
    }
}

impl<T> Deref for CowVec<T>
where
    T: Clone,
{
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
