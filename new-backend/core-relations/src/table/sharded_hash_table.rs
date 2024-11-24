//! Basic utilities around sharding a hashbrown `HashTable`.

use hashbrown::HashTable;
use numeric_id::{define_id, NumericId};

define_id!(pub(crate) ShardId, u32, "an identifier pointing to a shard in a sharded hash table");

/// Sharding metadata for a given [`ShardedHashTable`].
///
/// This is a separate type in order to allow other data-structures to pre-shard
/// data bound for a particular table.
#[derive(Copy, Clone)]
pub(crate) struct ShardData {
    log2_shard_count: u32,
}

impl ShardData {
    pub(crate) fn n_shards(&self) -> usize {
        1 << self.log2_shard_count
    }
    pub(crate) fn shard_id(&self, hash: u64) -> ShardId {
        let high_bits =
            (hash.wrapping_shr(64 - self.log2_shard_count)) & ((1 << self.log2_shard_count) - 1);
        ShardId::from_usize(high_bits as usize)
    }
}

#[derive(Clone)]
pub(crate) struct ShardedHashTable<T> {
    log2_shard_count: u32,
    shards: Vec<HashTable<T>>,
}

impl<T> Default for ShardedHashTable<T> {
    fn default() -> Self {
        Self::with_shards(rayon::current_num_threads())
    }
}

impl<T> ShardedHashTable<T> {
    pub(crate) fn clear(&mut self) {
        self.shards.iter_mut().for_each(|s| s.clear());
    }
    pub(crate) fn with_shards(shards: usize) -> Self {
        let log2_shard_count = shards.next_power_of_two().trailing_zeros();
        let shards = (0..(1 << log2_shard_count))
            .map(|_| HashTable::new())
            .collect::<Vec<_>>();
        Self {
            log2_shard_count,
            shards,
        }
    }

    /// Extract a [`ShardData`] allowing users to compute shard information for
    /// this table.
    pub(crate) fn shard_data(&self) -> ShardData {
        ShardData {
            log2_shard_count: self.log2_shard_count,
        }
    }

    pub(crate) fn get_shard(&self, shard_id: ShardId) -> &HashTable<T> {
        &self.shards[shard_id.index()]
    }

    pub(crate) fn mut_shards(&mut self) -> &mut [HashTable<T>] {
        &mut self.shards
    }
}
