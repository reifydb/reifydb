use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

/// WASM implementation of ConcurrentMap using Arc<RwLock<HashMap>>.
pub struct ConcurrentMap<K, V>
where
    K: Eq + Hash,
{
    inner: Arc<RwLock<HashMap<K, V>>>,
}

// SAFETY: The inner Arc<RwLock<HashMap>> is Sync, and we need to explicitly mark this
// for WASM targets where Sync is not automatically derived.
unsafe impl<K, V> Sync for ConcurrentMap<K, V>
where
    K: Eq + Hash + Send,
    V: Send,
{
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new empty concurrent map.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets the value for a key, or inserts it using the provided function if it doesn't exist.
    /// Returns a clone of the value.
    #[inline]
    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
        V: Clone,
        K: Clone,
    {
        // First try read lock to see if key exists
        {
            let map = self.inner.read().unwrap();
            if let Some(value) = map.get(&key) {
                return value.clone();
            }
        }

        // Key doesn't exist, acquire write lock
        let mut map = self.inner.write().unwrap();
        // Check again in case another thread inserted while we were waiting
        map.entry(key).or_insert_with(f).clone()
    }

    /// Gets a clone of the value associated with the key.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: Clone,
    {
        let map = self.inner.read().unwrap();
        map.get(key).cloned()
    }

    /// Returns true if the map contains the specified key.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let map = self.inner.read().unwrap();
        map.contains_key(key)
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
        let map = self.inner.read().unwrap();
        map.get(key).map(f)
    }

    /// Removes all entries from the map.
    #[inline]
    pub fn clear(&self) {
        let mut map = self.inner.write().unwrap();
        map.clear();
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
