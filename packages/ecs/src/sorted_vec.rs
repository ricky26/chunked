#![allow(dead_code)]

use std::ops::Deref;
use std::cmp::Ordering;

pub struct VecMapEntry<'a, K, V> {
    storage: &'a mut Vec<(K, V)>,
    index: usize,
    exists: bool,
    key: &'a K,
}

impl<'a, K: Clone, V> VecMapEntry<'a, K, V> {
    /// Returns true if the entry represents an existing item.
    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Returns a reference to the contents of the entry.
    pub fn get(&self) -> Option<&V> {
        if self.exists {
            Some(&self.storage[self.index].1)
        } else {
            None
        }
    }

    /// Returns a mutable reference to the contents of the entry.
    pub fn get_mut(&mut self) -> Option<&mut V> {
        if self.exists {
            Some(&mut self.storage[self.index].1)
        } else {
            None
        }
    }

    /// Set the contents of the given entry.
    pub fn set(&mut self, value: V) {
        if self.exists {
            self.storage[self.index].1 = value;
        } else {
            self.storage.insert(self.index, (self.key.clone(), value));
            self.exists = true;
        }
    }

    /// Clear the contents of the entry.
    pub fn delete(&mut self) {
        if self.exists {
            self.storage.remove(self.index);
            self.exists = false;
        }
    }
}

/// A set implemented on top of a sorted vector.
#[derive(Debug, Clone)]
pub struct VecSet<T> {
    inner: Vec<T>,
}

impl<T: Ord> VecSet<T> {
    /// Create a new `VecSet` with empty storage.
    pub fn new() -> VecSet<T> {
        VecSet {
            inner: Vec::new(),
        }
    }

    /// Create a new `VecSet` from an existing vector.
    /// 
    /// The vector will be sorted.
    pub fn from_inner(mut inner: Vec<T>) -> VecSet<T> {
        inner.sort();

        VecSet {
            inner,
        }
    }

    /// Destroy the `VecSet` returning the underlying `Vec`.
    pub fn into_inner(self) -> Vec<T> {
        self.inner
    }

    /// Returns true if this set contains the referenced value.
    pub fn has(&self, value: &T) -> bool {
        self.inner.binary_search(value).is_ok()
    }

    /// Returns the number of items in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the set contains no items.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Insert an item into the set.
    /// 
    /// Returns `true` if the item was a new addition.
    pub fn insert(&mut self, value: T) -> bool {
        match self.inner.binary_search(&value) {
            Ok(_) => false,
            Err(insert_idx) => {
                self.inner.insert(insert_idx, value);
                true
            },
        }
    }

    /// Remove an item from the set.
    /// 
    /// Returns true if the item was removed.
    pub fn remove(&mut self, value: &T) -> bool {
        match self.inner.binary_search(value) {
            Ok(idx) => {
                self.inner.remove(idx);
                true
            },
            Err(_) => false,
        }
    }

    /// Return a new set with the given value included.
    pub fn with(&self, value: T) -> VecSet<T>
        where T: Clone
    {
        match self.inner.binary_search(&value) {
            Ok(_) => self.clone(),
            Err(insert_idx) => {
                let mut new_inner = Vec::with_capacity(self.inner.len() + 1);
                new_inner.extend(self.inner[..insert_idx].iter().cloned());
                new_inner.push(value);
                new_inner.extend(self.inner[insert_idx..].iter().cloned());
                VecSet{
                    inner: new_inner,
                }
            },
        }
    }
}

impl<T> Deref for VecSet<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &*self.inner
    }
}

/// A map backed onto a sorted vector.
pub struct VecMap<K, V> {
    inner: Vec<(K, V)>,
}

impl <K: Ord, V> VecMap<K, V> {
    /// Create a new `VecMap` with empty storage.
    pub fn new() -> VecMap<K, V> {
        VecMap {
            inner: Vec::new(),
        }
    }

    /// Create a new `VecMap` from an existing vector.
    /// 
    /// The vector will be sorted.
    pub fn from_inner(mut inner: Vec<(K, V)>) -> VecMap<K, V> {
        inner.sort_by(|(k0, _), (k1, _)| k0.cmp(k1));

        VecMap {
            inner,
        }
    }

    /// Destroy the `VecMap` returning the underlying `Vec`.
    pub fn into_inner(self) -> Vec<(K, V)> {
        self.inner
    }

    /// Returns the number of pairs in the map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the map contains no pairs.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Binary search the collection for the given key.
    pub fn binary_search(&self, key: &K) -> Result<usize, usize> {
        self.inner.binary_search_by_key(&key, |(k, _)| k)
    }

    /// Returns true if the given key exists in this collection.
    pub fn has_key(&self, key: &K) -> bool {
        self.binary_search(key).is_ok()
    }

    /// Return a mutable entry for the given key.
    pub fn entry<'a>(&'a mut self, key: &'a K) -> VecMapEntry<'a, K, V> {
        match self.binary_search(key) {
            Ok(index) => VecMapEntry {
                storage: &mut self.inner,
                index,
                exists: true,
                key,
            },
            Err(index) => VecMapEntry {
                storage: &mut self.inner,
                index,
                exists: false,
                key,
            },
        }
    }

    /// Insert an item into the set, returns true if the key was new.
    pub fn insert(&mut self, key: K, value: V) -> bool {
        match self.binary_search(&key) {
            Ok(existing_idx) => {
                self.inner[existing_idx].1 = value;
                false
            },
            Err(insert_idx) => {
                self.inner.insert(insert_idx, (key, value));
                true
            },
        }
    }

    /// Remove an item from the set.
    /// 
    /// Returns true if the item was removed.
    pub fn remove(&mut self, key: &K) -> bool {
        match self.binary_search(&key) {
            Ok(index) => {
                self.inner.remove(index);
                true
            },
            Err(_) => false,
        }
    }
}

impl<K, V> Deref for VecMap<K, V> {
    type Target = [(K, V)];

    fn deref(&self) -> &[(K, V)] {
        &*self.inner
    }
}

/// A MultiMap backed onto a Vec.
pub struct VecMultiMap<K, V> {
    inner: Vec<(K, V)>,
}

impl <K: Ord, V> VecMultiMap<K, V> {
    /// Create a new `VecMultiMap` with empty storage.
    pub fn new() -> VecMultiMap<K, V> {
        VecMultiMap {
            inner: Vec::new(),
        }
    }

    /// Create a new `VecMultiMap` from an existing vector.
    /// 
    /// The vector will be sorted.
    pub fn from_inner(mut inner: Vec<(K, V)>) -> VecMultiMap<K, V> {
        inner.sort_by(|(k0, _), (k1, _)| k0.cmp(k1));

        VecMultiMap {
            inner,
        }
    }

    /// Destroy the `VecMultiMap` returning the underlying `Vec`.
    pub fn into_inner(self) -> Vec<(K, V)> {
        self.inner
    }

    /// Find the lower bound of entries for the given key.
    pub fn lower_bound(&self, key: &K) -> Result<usize, usize> {
        self.inner.binary_search_by_key(&key, |(k, _)| k)
    }

    /// Find the upper bound of entries for the given key.
    pub fn upper_bound(&self, key: &K) -> usize {
        self.inner.binary_search_by(|(k, _)| {
            match k.cmp(key) {
                Ordering::Equal => Ordering::Less,
                o => o,
            }
        }).unwrap_err()
    }

    /// Returns true if the given key exists in this collection.
    pub fn has_key(&self, key: &K) -> bool {
        self.lower_bound(key).is_ok()
    }

    /// Insert an item into the set.
    pub fn insert(&mut self, key: K, value: V) {
        let insert_idx = self.upper_bound(&key);
        self.inner.insert(insert_idx, (key, value));
    }

    /// Remove an item from the set.
    /// 
    /// Returns true if the item was removed.
    pub fn remove(&mut self, key: &K) -> bool {
        match self.lower_bound(key) {
            Ok(lower_bound) => {
                let upper_bound = self.upper_bound(key);
                self.inner.drain(lower_bound..upper_bound).count();
                true
            },
            Err(_) => false,
        }
    }
    
    /// List the entries associated with a given key.
    pub fn entries(&self, key: &K) -> &[(K, V)] {
        match self.lower_bound(key) {
            Ok(lower_bound) => {
                let upper_bound = self.upper_bound(key);
                &self.inner[lower_bound..upper_bound]
            },
            Err(_) => {
                &[]
            },
        }
    }
}

impl<K, V> Deref for VecMultiMap<K, V> {
    type Target = [(K, V)];

    fn deref(&self) -> &[(K, V)] {
        &*self.inner
    }
}
