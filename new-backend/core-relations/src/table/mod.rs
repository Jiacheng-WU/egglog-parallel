//! A generic table implementation supporting sorted writes.
//!
//! The primary difference between this table and the `Function` implementation
//! in egglog is that high level concepts like "timestamp" and "merge function"
//! are abstracted away from the core functionality of the table.

use std::{
    any::Any,
    cmp,
    hash::Hasher,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use crossbeam_queue::SegQueue;
use hashbrown::HashTable;
use numeric_id::{DenseIdMap, NumericId};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rustc_hash::FxHasher;
use sharded_hash_table::{ShardData, ShardId, ShardedHashTable};

use crate::{
    action::ExecutionState,
    common::Value,
    offsets::{OffsetRange, Offsets, RowId, Subset, SubsetRef},
    pool::with_pool_set,
    row_buffer::{ParallelRowBufWriter, RowBuffer},
    table_spec::{
        ColumnId, Constraint, Generation, MutationBuffer, Offset, Row, Table, TableSpec,
        TableVersion,
    },
    Pooled,
};

mod sharded_hash_table;
#[cfg(test)]
mod tests;

// NB: we currently only use 32 bits of hash code. To use 64, we can change
// `HashCode` here to `u64`.

type HashCode = u64;

/// A pointer to a row in the table.
#[derive(Clone, Debug)]
struct TableEntry {
    hashcode: HashCode,
    row: RowId,
}

impl TableEntry {
    fn hashcode(&self) -> u64 {
        // We keep the cast here to make it easy to switch to HashCode=u32.
        #[allow(clippy::unnecessary_cast)]
        {
            self.hashcode as u64
        }
    }
}

/// The core data for a table.
///
/// This type is a thin wrapper around `RowBuffer`. The big difference is that
/// it keeps track of how many stale rows are present.
#[derive(Clone)]
struct Rows {
    data: RowBuffer,
    stale_rows: usize,
}

impl Rows {
    fn new(data: RowBuffer) -> Rows {
        Rows {
            data,
            stale_rows: 0,
        }
    }
    fn clear(&mut self) {
        self.data.clear();
        self.stale_rows = 0;
    }
    fn next_row(&self) -> RowId {
        RowId::from_usize(self.data.len())
    }
    fn set_stale(&mut self, row: RowId) {
        if !self.data.set_stale(row) {
            self.stale_rows += 1;
        }
    }

    fn get_row(&self, row: RowId) -> Option<&[Value]> {
        let row = self.data.get_row(row);
        if row[0].is_stale() {
            None
        } else {
            Some(row)
        }
    }

    /// A variant of `get_row` without bounds-checking on `row`.
    unsafe fn get_row_unchecked(&self, row: RowId) -> Option<&[Value]> {
        let row = self.data.get_row_unchecked(row);
        if row[0].is_stale() {
            None
        } else {
            Some(row)
        }
    }

    fn add_row(&mut self, row: &[Value]) -> RowId {
        if row[0].is_stale() {
            self.stale_rows += 1;
        }
        self.data.add_row(row)
    }

    fn remove_stale(&mut self, remap: impl FnMut(&[Value], RowId, RowId)) {
        self.data.remove_stale(remap);
        self.stale_rows = 0;
    }
}

/// A callback that can perform merges for a table.
///
/// Merge functions get a handle to the current ExecutionState, the current
/// value, and the newly inserted row (in that order). Returns `true` if the
/// value was updated.
pub(crate) type MergeFn =
    Arc<dyn Fn(&mut ExecutionState, &[Value], &[Value], &mut Vec<Value>) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct SortedWritesTable {
    generation: Generation,
    data: Rows,
    hash: ShardedHashTable<TableEntry>,

    n_keys: usize,
    n_columns: usize,
    sort_by: Option<ColumnId>,
    offsets: Vec<(Value, RowId)>,

    pending_state: Arc<PendingState>,
    merge: MergeFn,
}

struct Buffer {
    pending_rows: DenseIdMap<ShardId, RowBuffer>,
    pending_removals: DenseIdMap<ShardId, RowBuffer>,
    state: Weak<PendingState>,
    n_cols: u32,
    n_keys: u32,
    shard_data: ShardData,
}

impl MutationBuffer for Buffer {
    fn stage_insert(&mut self, row: &[Value]) {
        let (shard, _) = hash_code(self.shard_data, row, self.n_keys as _);
        self.pending_rows
            .get_or_insert(shard, || RowBuffer::new(self.n_cols as _))
            .add_row(row);
    }
    fn stage_remove(&mut self, key: &[Value]) {
        let (shard, _) = hash_code(self.shard_data, key, self.n_keys as _);
        self.pending_removals
            .get_or_insert(shard, || RowBuffer::new(self.n_keys as _))
            .add_row(key);
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            for (staged, queues, counter) in [
                (
                    &mut self.pending_rows,
                    &state.pending_rows,
                    &state.total_rows,
                ),
                (
                    &mut self.pending_removals,
                    &state.pending_removals,
                    &state.total_removals,
                ),
            ] {
                let mut rows = 0;
                for shard_id in 0..staged.n_ids() {
                    let shard = ShardId::from_usize(shard_id);
                    let Some(buf) = staged.take(shard) else {
                        continue;
                    };
                    rows += buf.len();
                    queues[shard].push(buf);
                }
                counter.fetch_add(rows, Ordering::Relaxed);
            }
        }
    }
}

impl Table for SortedWritesTable {
    fn dyn_clone(&self) -> Box<dyn Table> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn clear(&mut self) {
        self.pending_state.clear();
        if self.data.data.len() == 0 {
            return;
        }
        self.offsets.clear();
        self.data.clear();
        self.hash.clear();
        self.generation = Generation::from_usize(self.version().major.index() + 1);
    }

    fn spec(&self) -> TableSpec {
        TableSpec {
            n_keys: self.n_keys,
            n_vals: self.n_columns - self.n_keys,
            uncacheable_columns: Default::default(),
            allows_delete: true,
        }
    }

    fn version(&self) -> TableVersion {
        TableVersion {
            major: self.generation,
            minor: Offset::from_usize(self.data.next_row().index()),
        }
    }

    fn updates_since(&self, gen: Offset) -> Subset {
        Subset::Dense(OffsetRange::new(
            RowId::from_usize(gen.index()),
            self.data.next_row(),
        ))
    }

    fn all(&self) -> Subset {
        Subset::Dense(OffsetRange::new(RowId::new(0), self.data.next_row()))
    }

    fn len(&self) -> usize {
        self.data.data.len() - self.data.stale_rows
    }

    fn scan_generic(&self, subset: SubsetRef, mut f: impl FnMut(RowId, &[Value]))
    where
        Self: Sized,
    {
        let Some((_low, hi)) = subset.bounds() else {
            // Empty subset
            return;
        };
        assert!(hi.index() <= self.data.data.len());
        // SAFETY: subsets are sorted, low must be at most hi, and hi is less
        // than the length of the table.
        subset.offsets(|row| unsafe {
            if let Some(vals) = self.data.get_row_unchecked(row) {
                f(row, vals)
            }
        })
    }

    fn scan_generic_bounded(
        &self,
        subset: SubsetRef,
        start: Offset,
        n: usize,
        cs: &[Constraint],
        mut f: impl FnMut(RowId, &[Value]),
    ) -> Option<Offset>
    where
        Self: Sized,
    {
        if cs.is_empty() {
            subset
                .iter_bounded(start.index(), start.index() + n, |row| {
                    let Some(entry) = self.data.get_row(row) else {
                        return;
                    };
                    f(row, entry);
                })
                .map(Offset::from_usize)
        } else {
            subset
                .iter_bounded(start.index(), start.index() + n, |row| {
                    let Some(entry) = self.get_if(cs, row) else {
                        return;
                    };
                    f(row, entry);
                })
                .map(Offset::from_usize)
        }
    }

    fn fast_subset(&self, constraint: &Constraint) -> Option<Subset> {
        let sort_by = self.sort_by?;
        match constraint {
            Constraint::Eq { .. } => None,
            Constraint::EqConst { col, val } => {
                if col == &sort_by {
                    match self.binary_search_sort_val(*val) {
                        Ok((found, bound)) => Some(Subset::Dense(OffsetRange::new(found, bound))),
                        Err(_) => Some(Subset::empty()),
                    }
                } else {
                    None
                }
            }
            Constraint::LtConst { col, val } => {
                if col == &sort_by {
                    match self.binary_search_sort_val(*val) {
                        Ok((found, _)) => {
                            Some(Subset::Dense(OffsetRange::new(RowId::new(0), found)))
                        }
                        Err(next) => Some(Subset::Dense(OffsetRange::new(RowId::new(0), next))),
                    }
                } else {
                    None
                }
            }
            Constraint::GtConst { col, val } => {
                if col == &sort_by {
                    match self.binary_search_sort_val(*val) {
                        Ok((_, bound)) => {
                            Some(Subset::Dense(OffsetRange::new(bound, self.data.next_row())))
                        }
                        Err(next) => {
                            Some(Subset::Dense(OffsetRange::new(next, self.data.next_row())))
                        }
                    }
                } else {
                    None
                }
            }
            Constraint::LeConst { col, val } => {
                if col == &sort_by {
                    match self.binary_search_sort_val(*val) {
                        Ok((_, bound)) => {
                            Some(Subset::Dense(OffsetRange::new(RowId::new(0), bound)))
                        }
                        Err(next) => Some(Subset::Dense(OffsetRange::new(RowId::new(0), next))),
                    }
                } else {
                    None
                }
            }
            Constraint::GeConst { col, val } => {
                if col == &sort_by {
                    match self.binary_search_sort_val(*val) {
                        Ok((found, _)) => {
                            Some(Subset::Dense(OffsetRange::new(found, self.data.next_row())))
                        }
                        Err(next) => {
                            Some(Subset::Dense(OffsetRange::new(next, self.data.next_row())))
                        }
                    }
                } else {
                    None
                }
            }
        }
    }

    fn refine_one(&self, mut subset: Subset, c: &Constraint) -> Subset {
        // NB: we aren't using any of the `fast_subset` tricks here. We may want
        // to if the higher-level implementations end up using it directly.
        subset.retain(|row| self.eval(std::slice::from_ref(c), row));
        subset
    }

    fn new_buffer(&self) -> Box<dyn MutationBuffer> {
        let n_shards = self.hash.shard_data().n_shards();
        Box::new(Buffer {
            pending_rows: DenseIdMap::with_capacity(n_shards),
            pending_removals: DenseIdMap::with_capacity(n_shards),
            state: Arc::downgrade(&self.pending_state),
            n_keys: u32::try_from(self.n_keys).expect("n_keys should fit in u32"),
            n_cols: u32::try_from(self.n_columns).expect("n_columns should fit in u32"),
            shard_data: self.hash.shard_data(),
        })
    }

    fn merge(&mut self, exec_state: &mut ExecutionState) -> bool {
        let mut changed = false;

        // First: handle the removals.
        changed |= self.do_delete();
        let double_check = {
            #[cfg(test)]
            {
                true
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        changed |= self.do_insert(exec_state, double_check);
        self.maybe_rehash();
        changed
    }

    fn get_row(&self, key: &[Value]) -> Option<Row> {
        let id = get_entry(key, self.n_keys, &self.hash, |row| {
            &self.data.get_row(row).unwrap()[0..self.n_keys] == key
        })?;
        let mut vals = with_pool_set(|ps| ps.get::<Vec<Value>>());
        vals.extend_from_slice(self.data.get_row(id).unwrap());
        Some(Row { id, vals })
    }

    fn get_row_column(&self, key: &[Value], col: ColumnId) -> Option<Value> {
        let id = get_entry(key, self.n_keys, &self.hash, |row| {
            &self.data.get_row(row).unwrap()[0..self.n_keys] == key
        })?;
        Some(self.data.get_row(id).unwrap()[col.index()])
    }
}

impl SortedWritesTable {
    /// Create a new [`SortedWritesTable`] with the given number of keys,
    /// columns, and an optional sort column.
    ///
    /// The `merge_fn` is used to evaluate conflicts when more than one row is
    /// inserted with the same primary key. The old and new proposed values are
    /// passed as the second and third arguments, respectively, with the
    /// function filling the final argument with the contents of the new row.
    /// The return value indicates whether or not the contents of the vector
    /// should be used.
    ///
    /// Merge functions can access the database via [`ExecutionState`].
    pub fn new(
        n_keys: usize,
        n_columns: usize,
        sort_by: Option<ColumnId>,
        merge_fn: impl Fn(&mut ExecutionState, &[Value], &[Value], &mut Vec<Value>) -> bool
            + 'static
            + Send
            + Sync,
    ) -> Self {
        let hash = ShardedHashTable::<TableEntry>::default();
        let shard_data = hash.shard_data();
        SortedWritesTable {
            generation: Generation::new(0),
            data: Rows::new(RowBuffer::new(n_columns)),
            hash,
            n_keys,
            n_columns,
            sort_by,
            offsets: Default::default(),
            pending_state: Arc::new(PendingState::new(shard_data)),
            merge: Arc::new(merge_fn),
        }
    }

    /// Flush all pending removals, in parallel.
    fn parallel_delete(&mut self) -> bool {
        let shard_data = self.hash.shard_data();
        let stale_delta: usize = self
            .hash
            .mut_shards()
            .par_iter_mut()
            .enumerate()
            .filter_map(|(shard_id, shard)| {
                let shard_id = ShardId::from_usize(shard_id);
                if self.pending_state.pending_removals[shard_id].is_empty() {
                    return None;
                }
                Some((shard_id, shard))
            })
            .map(|(shard_id, shard)| {
                let queue = &self.pending_state.pending_removals[shard_id];
                let mut marked_stale = 0;
                while let Some(buf) = queue.pop() {
                    for to_remove in buf.non_stale() {
                        let (actual_shard, hc) = hash_code(shard_data, to_remove, self.n_keys);
                        assert_eq!(actual_shard, shard_id);
                        if let Ok(entry) = shard.find_entry(hc, |entry| {
                            entry.hashcode == (hc as _)
                                && &self.data.get_row(entry.row).unwrap()[0..self.n_keys]
                                    == to_remove
                        }) {
                            let (ent, _) = entry.remove();
                            // SAFETY: The safety requirements of
                            // `set_stale_shared` are that there are no
                            // concurrent accesses to `row`. No other threads
                            // can access this row within this method because
                            // different `shards` partition the space
                            // (guaranteed by the assertion above), and we
                            // launch at most one thread per shard.
                            marked_stale +=
                                unsafe { !self.data.data.set_stale_shared(ent.row) } as usize;
                        }
                    }
                }
                marked_stale
            })
            .sum();
        // Update the stale count with the total marked stale.
        self.data.stale_rows += stale_delta;
        stale_delta > 0
    }
    fn serial_delete(&mut self) -> bool {
        let shard_data = self.hash.shard_data();
        let mut changed = false;
        self.hash
            .mut_shards()
            .iter_mut()
            .enumerate()
            .for_each(|(shard_id, shard)| {
                let shard_id = ShardId::from_usize(shard_id);
                let queue = &self.pending_state.pending_removals[shard_id];
                while let Some(buf) = queue.pop() {
                    for to_remove in buf.non_stale() {
                        let (actual_shard, hc) = hash_code(shard_data, to_remove, self.n_keys);
                        assert_eq!(actual_shard, shard_id);
                        if let Ok(entry) = shard.find_entry(hc, |entry| {
                            entry.hashcode == (hc as _)
                                && &self.data.get_row(entry.row).unwrap()[0..self.n_keys]
                                    == to_remove
                        }) {
                            let (ent, _) = entry.remove();
                            self.data.set_stale(ent.row);
                            changed = true;
                        }
                    }
                }
            });
        changed
    }

    fn do_delete(&mut self) -> bool {
        let total = self.pending_state.total_removals.swap(0, Ordering::Relaxed);
        if do_parallel(total) {
            self.parallel_delete()
        } else {
            self.serial_delete()
        }
    }

    fn do_insert(&mut self, exec_state: &mut ExecutionState, double_check: bool) -> bool {
        let total = self.pending_state.total_rows.swap(0, Ordering::Relaxed);
        self.data.data.reserve(total);
        if do_parallel(total) {
            if let Some(col) = self.sort_by {
                let expected_table = if double_check {
                    let mut x = self.clone();
                    x.pending_state = Arc::new(self.pending_state._deep_copy());
                    let start_row = RowId::from_usize(x.data.data.len());
                    x.serial_insert(exec_state);
                    Some((x, start_row))
                } else {
                    None
                };
                let res = self.parallel_insert(
                    exec_state,
                    SortChecker {
                        col,
                        current: None,
                        baseline: self.offsets.last().map(|(v, _)| *v),
                    },
                    total,
                );
                if let Some((expected_table, start_row)) = expected_table {
                    let expected_len =
                        expected_table.data.data.len() - expected_table.data.stale_rows;
                    let actual_len = self.data.data.len() - self.data.stale_rows;
                    assert_eq!(actual_len, expected_len, "uh oh!");
                    assert_eq!(self.offsets, expected_table.offsets, "uh oh!!");
                    let mut new_rows_expected = Vec::new();
                    let shard_data = self.hash.shard_data();
                    let n_keys = self.n_keys;
                    expected_table.scan_generic(
                        SubsetRef::Dense(OffsetRange {
                            start: start_row,
                            end: RowId::from_usize(expected_table.data.data.len()),
                        }),
                        |_, row| {
                            let (shard, _) = hash_code(shard_data, row, n_keys);
                            new_rows_expected.push((row.to_vec(), shard));
                        },
                    );

                    let mut new_rows_actual = Vec::new();
                    self.scan_generic(
                        SubsetRef::Dense(OffsetRange {
                            start: start_row,
                            end: RowId::from_usize(self.data.data.len()),
                        }),
                        |_, row| {
                            let (shard, _) = hash_code(shard_data, row, n_keys);
                            new_rows_actual.push((row.to_vec(), shard));
                        },
                    );
                    let sorted = |x: &Vec<_>| {
                        let mut x = x.clone();
                        x.sort();
                        x
                    };
                    assert_eq!(
                        sorted(&new_rows_actual),
                        sorted(&new_rows_expected),
                        "uh oh!!! (scanned from {start_row:?} to {actual_len}) unsorted actual={new_rows_actual:?} expected={new_rows_expected:?}"
                    );
                }
                res
            } else {
                let expected_table = if double_check {
                    let mut x = self.clone();
                    x.pending_state = Arc::new(self.pending_state._deep_copy());
                    x.do_insert(exec_state, false);
                    Some(x)
                } else {
                    None
                };
                let res = self.parallel_insert(exec_state, (), total);
                if let Some(expected_table) = expected_table {
                    let expected_len =
                        expected_table.data.data.len() - expected_table.data.stale_rows;
                    let actual_len = self.data.data.len() - self.data.stale_rows;
                    assert_eq!(actual_len, expected_len, "uh oh!");
                    assert_eq!(self.offsets, expected_table.offsets, "uh oh!!");
                }
                res
            }
        } else {
            self.serial_insert(exec_state)
        }
    }

    fn serial_insert(&mut self, exec_state: &mut ExecutionState) -> bool {
        let mut changed = false;
        let n_keys = self.n_keys;
        let mut scratch = with_pool_set(|ps| ps.get::<Vec<Value>>());
        for (_outer_shard, queue) in self.pending_state.pending_rows.iter() {
            if let Some(sort_by) = self.sort_by {
                while let Some(buf) = queue.pop() {
                    for query in buf.non_stale() {
                        let key = &query[0..n_keys];
                        let entry = get_entry_mut(query, n_keys, &mut self.hash, |row| {
                            let Some(row) = self.data.get_row(row) else {
                                return false;
                            };
                            &row[0..n_keys] == key
                        });

                        let sort_val = query[sort_by.index()];

                        if let Some(row) = entry {
                            // First case: overwriting an existing value. Apply merge
                            // function. Insert new row and update hash table if merge
                            // changes anything.
                            let cur = self
                                .data
                                .get_row(*row)
                                .expect("table should not point to stale entry");
                            if (self.merge)(exec_state, cur, query, &mut scratch) {
                                let new = self.data.add_row(&scratch);
                                if let Some(largest) = self.offsets.last().map(|(v, _)| *v) {
                                    assert!(sort_val >= largest, "inserting row that violates sort order ({sort_val:?} vs. {largest:?})");
                                    if sort_val > largest {
                                        self.offsets.push((sort_val, new));
                                    }
                                } else {
                                    self.offsets.push((sort_val, new));
                                }
                                self.data.set_stale(*row);
                                *row = new;
                                changed = true;
                            }
                            scratch.clear();
                        } else {
                            // New value: update invariants.
                            let new = self.data.add_row(query);
                            if let Some(largest) = self.offsets.last().map(|(v, _)| *v) {
                                assert!(
                                    sort_val >= largest,
                                    "inserting row that violates sort order"
                                );
                                if sort_val > largest {
                                    self.offsets.push((sort_val, new));
                                }
                            } else {
                                self.offsets.push((sort_val, new));
                            }
                            let (shard, hc) = hash_code(self.hash.shard_data(), query, self.n_keys);
                            debug_assert_eq!(shard, _outer_shard);
                            self.hash.mut_shards()[shard.index()].insert_unique(
                                hc as _,
                                TableEntry {
                                    hashcode: hc as _,
                                    row: new,
                                },
                                |entry| entry.hashcode(),
                            );
                            changed = true;
                        }
                    }
                }
            } else {
                // Simplified variant without the sorting constraint.
                while let Some(buf) = queue.pop() {
                    for query in buf.non_stale() {
                        let key = &query[0..n_keys];
                        let entry = get_entry_mut(query, n_keys, &mut self.hash, |row| {
                            let Some(row) = self.data.get_row(row) else {
                                return false;
                            };
                            &row[0..n_keys] == key
                        });

                        if let Some(row) = entry {
                            let cur = self
                                .data
                                .get_row(*row)
                                .expect("table should not point to stale entry");
                            if (self.merge)(exec_state, cur, query, &mut scratch) {
                                let new = self.data.add_row(&scratch);
                                self.data.set_stale(*row);
                                *row = new;
                                changed = true;
                            }
                            scratch.clear();
                        } else {
                            // New value: update invariants.
                            let new = self.data.add_row(query);
                            let (shard, hc) = hash_code(self.hash.shard_data(), query, self.n_keys);
                            debug_assert_eq!(shard, _outer_shard);
                            self.hash.mut_shards()[shard.index()].insert_unique(
                                hc as _,
                                TableEntry {
                                    hashcode: hc as _,
                                    row: new,
                                },
                                |entry| entry.hashcode(),
                            );
                            changed = true;
                        }
                    }
                }
            };
        }
        changed
    }

    fn parallel_insert<C: OrderingChecker>(
        &mut self,
        exec_state: &ExecutionState,
        checker: C,
        n_rows: usize,
    ) -> bool {
        // Parallel insert uses one giant parallel foreach. We have updates
        // pre-sharded, and one logical thread can process updates for each
        // shard independently. Updates happen in three phases, which comments
        // describe below.
        let shard_data = self.hash.shard_data();
        let n_keys = self.n_keys;
        let n_cols = self.n_columns;
        let next_offset = RowId::from_usize(self.data.data.len());
        let row_writer = self.data.data.parallel_writer();
        let pending_adds = self
            .hash
            .mut_shards()
            .par_iter_mut()
            .enumerate()
            .map(|(shard_id, shard)| {
                let shard_id = ShardId::from_usize(shard_id);
                let mut checker = checker.clone();
                let mut exec_state = exec_state.new_handle();
                let mut scratch = with_pool_set(|ps| ps.get::<Vec<Value>>());
                let queue = &self.pending_state.pending_rows[shard_id];
                let mut marked_stale = 0usize;
                let mut staged = StagedOutputs::new(n_keys, n_cols, n_rows);
                let mut work_done = 0;
                // Phase 1: process all incoming updates:
                // * Add new values to `staged`
                // * Removing entries in `shard` and mark them as stale in
                // `data` if they will be overwritten.
                while let Some(buf) = queue.pop() {
                    work_done += buf.len();
                    // We create a read_handle once per batch to avoid blocking
                    // too many threads if someone needs to resize the row
                    // writer.
                    let read_handle = row_writer.read_handle();
                    for row in buf.non_stale() {
                        let key = &row[0..n_keys];
                        let (_actual_shard, hash) = hash_code(shard_data, key, key.len());
                        assert_eq!(shard_id, _actual_shard);
                        match shard.find_entry(hash, |ent| {
                            ent.hashcode == hash as HashCode
                                && &read_handle.get_row(ent.row)[0..n_keys] == key
                        }) {
                            Ok(occ) => {
                                let cur = read_handle.get_row(occ.get().row);
                                // Need to run a merge function.
                                if (self.merge)(&mut exec_state, cur, row, &mut scratch) {
                                    checker.check_local(row);
                                    // SAFETY: The safety requirements of
                                    // `set_stale_shared` are that there are no
                                    // concurrent accesses to `row`. We have
                                    // exclusive access to this shard.
                                    unsafe {
                                        let _was_stale =
                                            read_handle.set_stale_shared(occ.get().row);
                                        debug_assert!(!_was_stale);
                                    };
                                    // We have a new entry. Stage it to be added
                                    // and then remove this entry.
                                    staged.insert(&scratch, |cur, new, out| {
                                        (self.merge)(&mut exec_state, cur, new, out)
                                    });
                                    occ.remove();
                                    marked_stale += 1;
                                }
                                scratch.clear()
                            }
                            Err(_) => {
                                checker.check_local(row);
                                // Stage this row to get inserted later.
                                staged.insert(row, |cur, new, out| {
                                    (self.merge)(&mut exec_state, cur, new, out)
                                });
                            }
                        }
                    }
                    if work_done > 20_000 {
                        // In high-scale microbenchmarks we've noticed that rayon can get locked up
                        // if any given chunk of work takes too long. We use this counter as a
                        // signal yield work to other workers, which seems to help avoid this.
                        rayon::yield_now();
                        work_done = 0;
                    }
                }
                // Phase 2: Write the staged rows to the row writer. This only
                // works due to the `ParallelRowBufWriter` machinery.
                let start_row = staged.write_output(&row_writer);
                // Phase 3: With the values buffered in the row buffer, we can
                // write them back to the shard, pointed to the correct rows.

                // In the serial implementation, we do phases 2 and 3 inline with
                // processing the incoming mutation, but separating them out
                // this way allows us to do a single write to the shared row
                // buffer, rather than one per row, which would cause
                // contention.
                let mut changed = marked_stale > 0;
                let mut cur_row = start_row;
                for row in staged.rows() {
                    changed = true;
                    let (_actual_shard, hc) = hash_code(shard_data, row, n_keys);
                    debug_assert_eq!(_actual_shard, shard_id);
                    #[cfg(debug_assertions)]
                    {
                        let read_handle = row_writer.read_handle();
                        assert!(shard
                            .find(hc, |ent| {
                                ent.hashcode == hc as HashCode
                                    && &read_handle.get_row(ent.row)[0..n_keys] == row
                            })
                            .is_none());
                        unsafe {
                            // (hackily) read the value we wrote at this row and
                            // check that it matches.
                            let data_raw = read_handle._data_offset_for_testing();
                            let actual_row = std::slice::from_raw_parts(
                                data_raw.add(cur_row.index() * self.n_columns),
                                self.n_columns,
                            );
                            assert_eq!(actual_row, row);
                        }
                    }

                    shard.insert_unique(
                        hc,
                        TableEntry {
                            hashcode: hc as _,
                            row: cur_row,
                        },
                        TableEntry::hashcode,
                    );
                    cur_row = cur_row.inc();
                }
                (checker, marked_stale, changed)
            })
            .collect_vec_list();
        mem::drop(row_writer);
        // Now we just need to reset our invariants.

        // Confirm none of the writes violated sort order and update the
        // `offsets` vector.
        let checker = C::check_global(pending_adds.iter().flatten().map(|(checker, _, _)| checker));
        checker.update_offsets(next_offset, &mut self.offsets);

        // Update the staleness counters.
        self.data.stale_rows += pending_adds
            .iter()
            .flatten()
            .map(|(_, stale, _)| *stale)
            .sum::<usize>();

        // Register any changes.
        let changed = pending_adds
            .iter()
            .flatten()
            .any(|(_, _, changed)| *changed);
        changed
    }

    fn binary_search_sort_val(&self, val: Value) -> Result<(RowId, RowId), RowId> {
        match self.offsets.binary_search_by_key(&val, |(v, _)| *v) {
            Ok(got) => Ok((
                self.offsets[got].1,
                self.offsets
                    .get(got + 1)
                    .map(|(_, r)| *r)
                    .unwrap_or(self.data.next_row()),
            )),
            Err(next) => Err(self
                .offsets
                .get(next)
                .map(|(_, id)| *id)
                .unwrap_or(self.data.next_row())),
        }
    }
    fn eval(&self, cs: &[Constraint], row: RowId) -> bool {
        self.get_if(cs, row).is_some()
    }

    fn get_if(&self, cs: &[Constraint], row: RowId) -> Option<&[Value]> {
        let row = self.data.get_row(row)?;
        let mut res = true;
        for constraint in cs {
            match constraint {
                Constraint::Eq { l_col, r_col } => res &= row[l_col.index()] == row[r_col.index()],
                Constraint::EqConst { col, val } => res &= row[col.index()] == *val,
                Constraint::LtConst { col, val } => res &= row[col.index()] < *val,
                Constraint::GtConst { col, val } => res &= row[col.index()] > *val,
                Constraint::LeConst { col, val } => res &= row[col.index()] <= *val,
                Constraint::GeConst { col, val } => res &= row[col.index()] >= *val,
            }
        }
        if res {
            Some(row)
        } else {
            None
        }
    }

    fn maybe_rehash(&mut self) {
        if self.data.stale_rows > cmp::max(16, self.data.data.len() / 2) {
            self.rehash();
        }
    }

    fn rehash(&mut self) {
        self.generation = Generation::from_usize(self.version().major.index() + 1);
        if let Some(sort_by) = self.sort_by {
            self.offsets.clear();
            self.data.remove_stale(|row, old, new| {
                let stale_entry = get_entry_mut(row, self.n_keys, &mut self.hash, |x| x == old)
                    .expect("non-stale entry not mapped in hash");
                *stale_entry = new;
                let sort_col = row[sort_by.index()];
                if let Some((max, _)) = self.offsets.last() {
                    if sort_col > *max {
                        self.offsets.push((sort_col, new));
                    }
                } else {
                    self.offsets.push((sort_col, new));
                }
            })
        } else {
            self.data.remove_stale(|row, old, new| {
                let stale_entry = get_entry_mut(row, self.n_keys, &mut self.hash, |x| x == old)
                    .expect("non-stale entry not mapped in hash");
                *stale_entry = new;
            })
        }
    }
}

fn get_entry(
    row: &[Value],
    n_keys: usize,
    table: &ShardedHashTable<TableEntry>,
    test: impl Fn(RowId) -> bool,
) -> Option<RowId> {
    let (shard, hash) = hash_code(table.shard_data(), row, n_keys);
    table
        .get_shard(shard)
        .find(hash, |ent| {
            ent.hashcode == hash as HashCode && test(ent.row)
        })
        .map(|ent| ent.row)
}

fn get_entry_mut<'a>(
    row: &[Value],
    n_keys: usize,
    table: &'a mut ShardedHashTable<TableEntry>,
    test: impl Fn(RowId) -> bool,
) -> Option<&'a mut RowId> {
    let (shard, hash) = hash_code(table.shard_data(), row, n_keys);
    table.mut_shards()[shard.index()]
        .find_mut(hash, |ent| {
            ent.hashcode == hash as HashCode && test(ent.row)
        })
        .map(|ent| &mut ent.row)
}

fn hash_code(shard_data: ShardData, row: &[Value], n_keys: usize) -> (ShardId, u64) {
    let mut hasher = FxHasher::default();
    for val in &row[0..n_keys] {
        hasher.write_usize(val.index());
    }
    let full_code = hasher.finish();
    // We keep this cast here to allow for experimenting with HashCode=u32.
    #[allow(clippy::unnecessary_cast)]
    (shard_data.shard_id(full_code), full_code as HashCode as u64)
}

/// A simple struct for packaging up pending mutations to a `SortedWritesTable`.
struct PendingState {
    pending_rows: DenseIdMap<ShardId, SegQueue<RowBuffer>>,
    pending_removals: DenseIdMap<ShardId, SegQueue<RowBuffer>>,
    total_removals: AtomicUsize,
    total_rows: AtomicUsize,
}

impl PendingState {
    fn new(shard_data: ShardData) -> PendingState {
        let n_shards = shard_data.n_shards();
        let mut pending_rows = DenseIdMap::with_capacity(n_shards);
        let mut pending_removals = DenseIdMap::with_capacity(n_shards);
        for i in 0..n_shards {
            pending_rows.insert(ShardId::from_usize(i), SegQueue::default());
            pending_removals.insert(ShardId::from_usize(i), SegQueue::default());
        }

        PendingState {
            pending_rows,
            pending_removals,
            total_removals: AtomicUsize::new(0),
            total_rows: AtomicUsize::new(0),
        }
    }
    fn clear(&self) {
        for (_, queue) in self.pending_rows.iter() {
            while queue.pop().is_some() {}
        }

        for (_, queue) in self.pending_removals.iter() {
            while queue.pop().is_some() {}
        }
    }

    /// This is only really used in debugging, but it's annoying enough to write
    /// that it may help to have around.
    fn _deep_copy(&self) -> PendingState {
        let mut pending_rows = DenseIdMap::new();
        let mut pending_removals = DenseIdMap::new();
        fn drain_queue<T>(queue: &SegQueue<T>) -> Vec<T> {
            let mut res = Vec::new();
            while let Some(x) = queue.pop() {
                res.push(x);
            }
            res
        }
        for (shard, queue) in self.pending_rows.iter() {
            let contents = drain_queue(queue);
            let new_queue = SegQueue::default();
            for x in contents {
                new_queue.push(x.clone());
                queue.push(x);
            }
            pending_rows.insert(shard, new_queue);
        }

        for (shard, queue) in self.pending_removals.iter() {
            let contents = drain_queue(queue);
            let new_queue = SegQueue::default();
            for x in contents {
                new_queue.push(x.clone());
                queue.push(x);
            }
            pending_removals.insert(shard, new_queue);
        }

        PendingState {
            pending_rows,
            pending_removals,
            total_removals: AtomicUsize::new(self.total_removals.load(Ordering::Acquire)),
            total_rows: AtomicUsize::new(self.total_rows.load(Ordering::Acquire)),
        }
    }
}

/// A trait that encapsulates the logic of potentially checking that written
/// columns appear in sorted order.
///
/// For rows that are sorted by a column, an OrderingChecker asserts that all
/// new rows have the same value in that column, and that the column is greater
/// than or equal to the column value coming in. For rows not sorted, these
/// checks become no-ops.
trait OrderingChecker: Clone + Send + Sync {
    /// Check any invariants locally, updating the state of the checker when
    /// doing so.
    fn check_local(&mut self, row: &[Value]);
    /// Combine the states of multiple checkers, returning a new checker with
    /// all information assimilated. This is the checker that is suitable for
    /// calling `update_offsets` with.
    fn check_global<'a>(checkers: impl Iterator<Item = &'a Self>) -> Self
    where
        Self: 'a;
    /// Update the sorted offset vector with the current state of the checker.
    fn update_offsets(&self, start: RowId, offsets: &mut Vec<(Value, RowId)>);
}

impl OrderingChecker for () {
    fn check_local(&mut self, _: &[Value]) {}
    fn check_global<'a>(_: impl Iterator<Item = &'a ()>) {}
    fn update_offsets(&self, _: RowId, _: &mut Vec<(Value, RowId)>) {}
}

#[derive(Copy, Clone)]
struct SortChecker {
    col: ColumnId,
    baseline: Option<Value>,
    current: Option<Value>,
}

impl OrderingChecker for SortChecker {
    fn check_local(&mut self, row: &[Value]) {
        let val = row[self.col.index()];
        if let Some(cur) = self.current {
            assert_eq!(
                cur, val,
                "concurrently inserting rows with different sort keys"
            );
        } else {
            self.current = Some(val);
            if let Some(baseline) = self.baseline {
                assert!(val >= baseline, "inserted row violates sort order");
            }
        }
    }

    fn check_global<'a>(mut checkers: impl Iterator<Item = &'a Self>) -> Self {
        let Some(start) = checkers.next() else {
            return SortChecker {
                col: ColumnId::new(!0),
                baseline: None,
                current: None,
            };
        };
        let mut expected = start.current;
        for checker in checkers {
            assert_eq!(checker.baseline, start.baseline);
            match (&mut expected, checker.current) {
                (None, None) => {}
                (cur @ None, Some(x)) => {
                    *cur = Some(x);
                }
                (Some(_), None) => {}
                (Some(x), Some(y)) => {
                    assert_eq!(
                        *x, y,
                        "concurrently inserting rows with different sort keys"
                    );
                }
            }
        }
        SortChecker {
            col: start.col,
            baseline: start.baseline,
            current: expected,
        }
    }

    fn update_offsets(&self, start: RowId, offsets: &mut Vec<(Value, RowId)>) {
        if let Some(cur) = self.current {
            if let Some((max, _)) = offsets.last() {
                if cur > *max {
                    offsets.push((cur, start));
                }
            } else {
                offsets.push((cur, start));
            }
        }
    }
}

fn do_parallel(_workload_size: usize) -> bool {
    #[cfg(test)]
    {
        // In tests, run serial and parallel variants half the time,
        // nondeterministically.
        use rand::{thread_rng, Rng};
        thread_rng().gen::<bool>()
    }

    #[cfg(not(test))]
    {
        _workload_size > 50_000 && rayon::current_num_threads() > 1
    }
}

/// A type similar to a SortedWritesTable used to buffer outputs. The main thing
/// that StagedOutputs handles is running the merge function for a table on
/// multiple updates to the same key that show up in the same round of
/// insertions.
struct StagedOutputs {
    shard_data: ShardData,
    n_keys: usize,
    hash: HashTable<TableEntry>,
    rows: RowBuffer,
    n_stale: usize,
    scratch: Pooled<Vec<Value>>,
}

impl StagedOutputs {
    fn rows(&self) -> impl Iterator<Item = &[Value]> {
        self.rows.non_stale()
    }
    fn new(n_keys: usize, n_cols: usize, capacity: usize) -> Self {
        let mut res = StagedOutputs {
            shard_data: ShardData::new(1),
            n_keys,
            n_stale: 0,
            hash: HashTable::with_capacity(capacity),
            rows: RowBuffer::new(n_cols),
            scratch: with_pool_set(|ps| ps.get::<Vec<Value>>()),
        };

        res.rows.reserve(capacity);
        res
    }

    fn insert(
        &mut self,
        row: &[Value],
        mut merge_fn: impl FnMut(&[Value], &[Value], &mut Vec<Value>) -> bool,
    ) {
        use hashbrown::hash_table::Entry;
        let (_, hc) = hash_code(self.shard_data, row, self.n_keys);
        let entry = self.hash.entry(
            hc,
            |te| {
                te.hashcode() == hc
                    && self.rows.get_row(te.row)[0..self.n_keys] == row[0..self.n_keys]
            },
            TableEntry::hashcode,
        );
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let cur = self.rows.get_row(occupied_entry.get().row);
                if merge_fn(cur, row, &mut self.scratch) {
                    let new = self.rows.add_row(&self.scratch);
                    self.rows.set_stale(occupied_entry.get().row);
                    self.n_stale += 1;
                    occupied_entry.get_mut().row = new;
                }
                self.scratch.clear();
            }
            Entry::Vacant(vacant_entry) => {
                let next = self.rows.add_row(row);
                vacant_entry.insert(TableEntry {
                    hashcode: hc as _,
                    row: next,
                });
            }
        }
    }

    /// Write the contents of the staged outputs to the given writer, returning
    /// the initial RowId of the new output.
    fn write_output(&self, output: &ParallelRowBufWriter) -> RowId {
        let n_rows = self.rows.len() - self.n_stale;
        let n_vals = n_rows * self.rows.arity();
        output.write_raw_values(
            WithExactSize {
                iter: self.rows.non_stale().flatten().copied(),
                size: n_vals,
            },
            n_rows,
        )
    }
}

/// A simple type used to attach a known size to an arbitrary iterator.
struct WithExactSize<I> {
    iter: I,
    size: usize,
}

impl<I: Iterator> Iterator for WithExactSize<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<I: Iterator> ExactSizeIterator for WithExactSize<I> {
    fn len(&self) -> usize {
        self.size
    }
}
