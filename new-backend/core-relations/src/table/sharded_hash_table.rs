//! Basic utilities around sharding a hashbrown `HashTable`.

use hashbrown::HashTable;
use numeric_id::{define_id, NumericId};

define_id!(pub(crate) ShardId, u32, "an identifier pointing to a shard in a sharded hash table");

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

    pub(crate) fn shard_id(&self, hash: u64) -> ShardId {
        let high_bits = (hash >> (64 - self.log2_shard_count)) & ((1 << self.log2_shard_count) - 1);
        ShardId::from_usize(high_bits as usize)
    }

    pub(crate) fn get_shard(&self, shard_id: ShardId) -> &HashTable<T> {
        &self.shards[shard_id.index()]
    }

    pub(crate) fn mut_shards(&mut self) -> &mut [HashTable<T>] {
        &mut self.shards
    }
}
