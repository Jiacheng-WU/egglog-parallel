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
    sync::{Arc, Weak},
};

use crossbeam_queue::SegQueue;
use numeric_id::NumericId;
use rustc_hash::FxHasher;
use sharded_hash_table::{ShardId, ShardedHashTable};

use crate::{
    action::ExecutionState,
    common::Value,
    offsets::{OffsetRange, Offsets, RowId, Subset, SubsetRef},
    pool::with_pool_set,
    row_buffer::RowBuffer,
    table_spec::{
        ColumnId, Constraint, Generation, MutationBuffer, Offset, Row, Table, TableSpec,
        TableVersion,
    },
};

mod sharded_hash_table;
#[cfg(test)]
mod tests;

// NB: we currently only use 32 bits of hash code. To use 64, we can change
// `HashCode` here to `u64`.

type HashCode = u32;

/// A pointer to a row in the table.
#[derive(Clone, Debug)]
struct TableEntry {
    hashcode: HashCode,
    row: RowId,
}

impl TableEntry {
    fn hashcode(&self) -> u64 {
        self.hashcode as u64
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
    pending_rows: RowBuffer,
    pending_removals: RowBuffer,
    state: Weak<PendingState>,
}

impl MutationBuffer for Buffer {
    fn stage_insert(&mut self, row: &[Value]) {
        self.pending_rows.add_row(row);
    }
    fn stage_remove(&mut self, key: &[Value]) {
        self.pending_removals.add_row(key);
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let pending_rows = mem::replace(&mut self.pending_rows, RowBuffer::new(1));
            let pending_removals = mem::replace(&mut self.pending_removals, RowBuffer::new(1));
            state.pending_removals.push(pending_removals);
            state.pending_rows.push(pending_rows);
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
        Box::new(Buffer {
            pending_rows: RowBuffer::new(self.n_columns),
            pending_removals: RowBuffer::new(self.n_keys),
            state: Arc::downgrade(&self.pending_state),
        })
    }

    fn merge(&mut self, exec_state: &mut ExecutionState) -> bool {
        let mut changed = false;

        let n_keys = self.n_keys;
        let mut scratch = with_pool_set(|ps| ps.get::<Vec<Value>>());

        // First: handle the removals.
        while let Some(buf) = self.pending_state.pending_removals.pop() {
            for to_remove in buf.non_stale() {
                let (shard_id, hc) = hash_code(&self.hash, to_remove, n_keys);
                let table = &mut self.hash.mut_shards()[shard_id.index()];
                if let Ok(entry) = table.find_entry(hc, |entry| {
                    entry.hashcode == (hc as _)
                        && &self.data.get_row(entry.row).unwrap()[0..n_keys] == to_remove
                }) {
                    let (ent, _) = entry.remove();
                    changed = true;
                    self.data.set_stale(ent.row);
                }
            }
        }

        // Now the insertions.
        if let Some(sort_by) = self.sort_by {
            while let Some(buf) = self.pending_state.pending_rows.pop() {
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
                        // function. Insert new row  and update hash table if merge
                        // changes anything.
                        let cur = self
                            .data
                            .get_row(*row)
                            .expect("table should not point to stale entry");
                        if (self.merge)(exec_state, cur, query, &mut scratch) {
                            let new = self.data.add_row(&scratch);
                            if let Some(largest) = self.offsets.last().map(|(v, _)| *v) {
                                assert!(
                                sort_val >= largest,
                                "inserting row that violates sort order ({sort_val:?} vs. {largest:?})"
                            );
                                if sort_val > largest {
                                    self.offsets.push((sort_val, new));
                                }
                            } else {
                                self.offsets.push((sort_val, new));
                            }
                            scratch.clear();
                            self.data.set_stale(*row);
                            *row = new;
                            changed = true;
                        }
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
                        let (shard, hc) = hash_code(&self.hash, query, self.n_keys);
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
            while let Some(buf) = self.pending_state.pending_rows.pop() {
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
                            scratch.clear();
                            self.data.set_stale(*row);
                            *row = new;
                            changed = true;
                        }
                    } else {
                        // New value: update invariants.
                        let new = self.data.add_row(query);
                        let (shard, hc) = hash_code(&self.hash, query, self.n_keys);
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
        SortedWritesTable {
            generation: Generation::new(0),
            data: Rows::new(RowBuffer::new(n_columns)),
            hash: Default::default(),
            n_keys,
            n_columns,
            sort_by,
            offsets: Default::default(),
            pending_state: Arc::new(PendingState::default()),
            merge: Arc::new(merge_fn),
        }
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
    let (shard, hash) = hash_code(table, row, n_keys);
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
    let (shard, hash) = hash_code(table, row, n_keys);
    table.mut_shards()[shard.index()]
        .find_mut(hash, |ent| {
            ent.hashcode == hash as HashCode && test(ent.row)
        })
        .map(|ent| &mut ent.row)
}

fn hash_code(table: &ShardedHashTable<TableEntry>, row: &[Value], n_keys: usize) -> (ShardId, u64) {
    let mut hasher = FxHasher::default();
    for val in &row[0..n_keys] {
        hasher.write_usize(val.index());
    }
    let full_code = hasher.finish();
    (table.shard_id(full_code), full_code as HashCode as u64)
}

/// A simple struct for packaging up pending mutations to a `SortedWritesTable`.
#[derive(Default)]
struct PendingState {
    pending_rows: SegQueue<RowBuffer>,
    pending_removals: SegQueue<RowBuffer>,
}

impl PendingState {
    fn clear(&self) {
        while self.pending_rows.pop().is_some() {}
        while self.pending_removals.pop().is_some() {}
    }
}
