use std::borrow::Borrow;
use std::hash::Hash;

use dashmap::DashMap;

/// Native implementation of ConcurrentMap using DashMap for high-performance concurrent access.
pub struct ConcurrentMap<K, V>
where
    K: Eq + Hash,
{
    inner: DashMap<K, V>,
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new empty concurrent map.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Gets the value for a key, or inserts it using the provided function if it doesn't exist.
    /// Returns a clone of the value.
    #[inline]
    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
        V: Clone,
    {
        self.inner.entry(key).or_insert_with(f).value().clone()
    }

    /// Gets a clone of the value associated with the key.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        self.inner.get(key).map(|guard| guard.value().clone())
    }

    /// Returns true if the map contains the specified key.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }

    /// Applies a closure to the value associated with the key, returning the result.
    /// Returns None if the key doesn't exist.
    #[inline]
    pub fn with_read<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        F: FnOnce(&V) -> R,
    {
        self.inner.get(key).map(|guard| f(guard.value()))
    }

    /// Removes all entries from the map.
    #[inline]
    pub fn clear(&self) {
        self.inner.clear();
    }
}

impl<K, V> Default for ConcurrentMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
