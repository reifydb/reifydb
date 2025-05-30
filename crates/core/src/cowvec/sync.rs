// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::ops::Deref;
use std::rc::Rc;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct CowVec<T>
where
    T: Clone,
{
    inner: Rc<Vec<T>>,
}

impl<T: Clone + PartialEq> PartialEq<[T]> for &CowVec<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.inner.as_slice() == other
    }
}

impl<T: Clone + PartialEq> PartialEq<[T]> for CowVec<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.inner.as_slice() == other
    }
}

impl<T: Clone + PartialEq> PartialEq<CowVec<T>> for [T] {
    fn eq(&self, other: &CowVec<T>) -> bool {
        self == other.inner.as_slice()
    }
}

impl<T: Clone + PartialEq> Clone for CowVec<T> {
    fn clone(&self) -> Self {
        CowVec { inner: Rc::clone(&self.inner) }
    }
}

impl<T: Clone + PartialEq> CowVec<T> {
    pub fn new(vec: Vec<T>) -> Self {
        CowVec { inner: Rc::new(vec) }
    }

    pub fn from_rc(rc: Rc<Vec<T>>) -> Self {
        CowVec { inner: rc }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.inner
    }

    pub fn is_owned(&self) -> bool {
        Rc::strong_count(&self.inner) == 1
    }

    pub fn is_shared(&self) -> bool {
        Rc::strong_count(&self.inner) > 1
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.inner.get(idx)
    }

    /// Ensures unique ownership and returns a mutable reference to the Vec.
    pub fn make_mut(&mut self) -> &mut Vec<T> {
        if Rc::strong_count(&self.inner) != 1 {
            let cloned = (*self.inner).clone();
            self.inner = Rc::new(cloned);
        }
        Rc::get_mut(&mut self.inner).expect("Rc::get_mut should succeed after ensuring uniqueness")
    }

    pub fn set(&mut self, idx: usize, value: T) {
        self.make_mut()[idx] = value;
    }

    pub fn push(&mut self, value: T) {
        self.make_mut().push(value);
    }

    pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) {
        self.make_mut().extend(iter);
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
}

impl<T: Clone + PartialEq> IntoIterator for CowVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        (*self.inner).clone().into_iter()
    }
}

impl<T: Clone + PartialEq> Deref for CowVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use crate::cowvec::sync::CowVec;

    #[test]
    fn test_new() {
        let cow = CowVec::new(vec![1, 2, 3]);
        assert_eq!(cow.get(0), Some(&1));
        assert_eq!(cow.get(1), Some(&2));
        assert_eq!(cow.get(2), Some(&3));
    }

    #[test]
    fn test_is_owned() {
        let owned = CowVec::new(vec![1, 2]);
        assert_eq!(owned.is_owned(), true);

        let shared = owned.clone();
        assert_eq!(owned.is_owned(), false);
        assert_eq!(shared.is_owned(), false);

        drop(shared);

        assert_eq!(owned.is_owned(), true);
    }

    #[test]
    fn test_is_shared() {
        let owned = CowVec::new(vec![1, 2]);
        assert_eq!(owned.is_shared(), false);

        let shared = owned.clone();
        assert_eq!(owned.is_shared(), true);
        assert_eq!(shared.is_shared(), true);

        drop(shared);

        assert_eq!(owned.is_shared(), false);
    }

    #[test]
    fn test_extend() {
        let mut owned = CowVec::new(vec![1, 2]);

        let ptr_before_owned = ptr_of(&owned);
        owned.extend([9, 9, 24]);
        assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
        assert_eq!(owned.len(), 5);

        let mut shared = owned.clone();

        let ptr_before_shared = ptr_of(&shared);
        shared.extend([9, 9, 24]);
        assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
        assert_eq!(owned.len(), 5);
    }

    #[test]
    fn test_push() {
        let mut owned = CowVec::new(vec![1, 2]);

        let ptr_before_owned = ptr_of(&owned);
        owned.push(99);
        assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
        assert_eq!(owned.len(), 3);

        let mut shared = owned.clone();

        let ptr_before_shared = ptr_of(&shared);
        shared.push(99);
        assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
        assert_eq!(owned.len(), 3);
    }

    #[test]
    fn test_set() {
        let mut owned = CowVec::new(vec![1, 2]);

        let ptr_before_owned = ptr_of(&owned);
        owned.set(1, 99);
        assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
        assert_eq!(*owned, [1, 99]);

        let mut shared = owned.clone();

        let ptr_before_shared = ptr_of(&shared);
        shared.set(1, 99);
        assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
        assert_eq!(*owned, [1, 99]);
    }

    #[test]
    fn test_reorder() {
        let mut owned = CowVec::new(vec![1, 2]);

        let ptr_before_owned = ptr_of(&owned);
        owned.reorder(&[1usize, 0]);
        assert_eq!(ptr_before_owned, ptr_of(&owned)); // no copy
        assert_eq!(*owned, [2, 1]);

        let mut shared = owned.clone();

        let ptr_before_shared = ptr_of(&shared);
        shared.reorder(&[1usize, 0]);
        assert_ne!(ptr_before_shared, ptr_of(&shared)); // copy-on-write
        assert_eq!(*shared, [1, 2]);
    }

    #[test]
    fn test_reorder_identity() {
        let mut cow = CowVec::new(vec![10, 20, 30]);
        cow.reorder(&[0, 1, 2]); // no-op
        assert_eq!(cow.as_slice(), &[10, 20, 30]);
    }

    #[test]
    fn test_reorder_basic() {
        let mut cow = CowVec::new(vec![10, 20, 30]);
        cow.reorder(&[2, 0, 1]);
        assert_eq!(cow.as_slice(), &[30, 10, 20]);
    }

    fn ptr_of(v: &CowVec<i32>) -> *const i32 {
        v.as_slice().as_ptr()
    }
}
