use std::{
    hash::{BuildHasherDefault, Hash, Hasher},
    mem,
    ops::Deref,
    sync::{Arc, Mutex},
};

use concurrency::ConcurrentVec;
use hashbrown::HashTable;
use numeric_id::{define_id, DenseIdMap, NumericId};
use rustc_hash::FxHasher;

use crate::pool::Clear;

pub(crate) type HashMap<K, V> = hashbrown::HashMap<K, V, BuildHasherDefault<FxHasher>>;
pub(crate) type HashSet<T> = hashbrown::HashSet<T, BuildHasherDefault<FxHasher>>;
pub(crate) type IndexSet<T> = indexmap::IndexSet<T, BuildHasherDefault<FxHasher>>;
pub(crate) type IndexMap<K, V> = indexmap::IndexMap<K, V, BuildHasherDefault<FxHasher>>;
pub(crate) type DashMap<K, V> = dashmap::DashMap<K, V, BuildHasherDefault<FxHasher>>;

/// An intern table mapping a key to some numeric id type.
///
/// This is primarily used to manage the [`Value`]s associated with a a
/// primtiive value.
#[derive(Clone)]
pub struct InternTable<K, V> {
    vals: Arc<ConcurrentVec<K>>,
    data: Vec<Arc<Mutex<HashTable<V>>>>,
    shards_log2: u32,
}

impl<K, V> Default for InternTable<K, V> {
    fn default() -> Self {
        Self::with_shards(4)
    }
}
impl<K, V> InternTable<K, V> {
    /// Create a new intern table with the given number of shards.
    ///
    /// The number of shards is passed as its base-2 log: we rely on the number
    /// of shards being a power of two.
    fn with_shards(shards_log2: u32) -> InternTable<K, V> {
        let mut data = Vec::new();
        data.resize_with(1 << shards_log2, Default::default);
        InternTable {
            vals: Arc::new(ConcurrentVec::with_capacity(512)),
            data,
            shards_log2,
        }
    }
}

impl<K: Eq + Hash + Clone, V: NumericId> InternTable<K, V> {
    pub fn intern(&self, k: &K) -> V {
        let hash = hash_value(k);
        // Use the top bits of the hash to pick the shard. Hashbrown uses the
        // bottom bits.
        let shard = ((hash >> (64 - self.shards_log2)) & ((1 << self.shards_log2) - 1)) as usize;
        let mut table = self.data[shard].lock().unwrap();
        let read_guard = self.vals.read();
        if let Some(v) = table.find(hash, |v| k == &read_guard[v.index()]) {
            *v
        } else {
            mem::drop(read_guard);
            let res = V::from_usize(self.vals.push(k.clone()));
            let read_guard = self.vals.read();
            *table
                .insert_unique(hash, res, |v| hash_value(&read_guard[v.index()]))
                .get()
        }
    }

    pub fn get(&self, v: V) -> impl Deref<Target = K> + '_ {
        MapDeref {
            base: self.vals.read(),
            index: v.index(),
        }
    }
}

fn hash_value(v: &impl Hash) -> u64 {
    let mut hasher = FxHasher::default();
    v.hash(&mut hasher);
    hasher.finish()
}

impl<K: NumericId, V> Clear for DenseIdMap<K, V> {
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }

    fn clear(&mut self) {
        self.clear();
    }
}

define_id!(pub Value, u32, "A generic identifier representing an egglog value");

impl Value {
    pub(crate) fn stale() -> Self {
        Value::new(u32::MAX)
    }
    /// Values have a special "Stale" value that is used to indicate that the
    /// value isn't intended to be read.
    pub(crate) fn set_stale(&mut self) {
        self.rep = u32::MAX;
    }

    /// Whether or not the given value is stale. See [`Value::set_stale`].
    pub(crate) fn is_stale(&self) -> bool {
        self.rep == u32::MAX
    }
}

struct MapDeref<T> {
    base: T,
    index: usize,
}

impl<S, T: Deref<Target = [S]>> Deref for MapDeref<T> {
    type Target = S;

    fn deref(&self) -> &S {
        &(&*self.base)[self.index]
    }
}
