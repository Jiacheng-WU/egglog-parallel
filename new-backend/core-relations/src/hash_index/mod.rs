//! Indexes on specific projections of tables.
use std::{
    hash::{Hash, Hasher},
    mem,
    ops::Deref,
};

use concurrency::{parallel_writer::UnsafeReadAccess, ConcurrentVec, ParallelVecWriter};
use crossbeam_queue::SegQueue;
use hashbrown::HashTable;
use numeric_id::{define_id, NumericId};
use rustc_hash::FxHasher;

use crate::{
    common::{IndexMap, Value},
    offsets::{RowId, SortedOffsetSlice, SubsetRef},
    pool::{with_pool_set, Clear, Pooled},
    row_buffer::{RowBuffer, TaggedRowBuffer},
    table_spec::{ColumnId, Generation, Offset, TableVersion, WrappedTable},
    OffsetRange,
};

#[cfg(test)]
mod tests;
struct TableEntry<T> {
    hash: u64,
    /// Points into `keys`
    key: RowId,
    vals: T,
}

pub(crate) struct Index<TI> {
    key: Vec<ColumnId>,
    updated_to: TableVersion,
    table: TI,
}

impl<TI: IndexBase> Index<TI> {
    pub(crate) fn new(key: Vec<ColumnId>, table: TI) -> Self {
        Index {
            key,
            updated_to: TableVersion {
                major: Generation::new(0),
                minor: Offset::new(0),
            },
            table,
        }
    }

    /// Get the nonempty subset of rows associated with this key, if there is
    /// one.
    pub(crate) fn get_subset<'a>(
        &'a self,
        key: &'a TI::Key,
    ) -> Option<impl Deref<Target = SubsetRef<'a>>> {
        self.table.get_subset(key)
    }

    pub(crate) fn needs_refresh(&self, table: &WrappedTable) -> bool {
        table.version() != self.updated_to
    }

    /// Update the contents of the index to the current version of the table.
    ///
    /// The index is guaranteed to be up to date until `merge` is called on the
    /// table again.
    pub(crate) fn refresh(&mut self, table: &WrappedTable) {
        let cur_version = table.version();
        if cur_version == self.updated_to {
            return;
        }
        let subset = if cur_version.major != self.updated_to.major {
            self.table.clear();
            table.all()
        } else {
            table.updates_since(self.updated_to.minor)
        };
        let mut buf = TaggedRowBuffer::new(self.key.len());
        let mut cur = Offset::new(0);
        loop {
            buf.clear();
            if let Some(next) =
                table.scan_project(subset.as_ref(), &self.key, cur, 1024, &[], &mut buf)
            {
                cur = next;
                self.table.merge_rows(&buf);
            } else {
                self.table.merge_rows(&buf);
                break;
            }
        }
        self.updated_to = cur_version;
    }

    pub(crate) fn for_each(&self, f: impl FnMut(&TI::Key, SubsetRef)) {
        self.table.for_each(f);
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }
}

// Define a newtype to hook things into the PoolSet machinery.
#[derive(Default)]
pub(crate) struct SubsetTable(HashTable<TableEntry<BufferedSubset>>);

impl Clear for SubsetTable {
    fn clear(&mut self) {
        self.0.clear();
    }
    fn reuse(&self) -> bool {
        self.0.capacity() > 0
    }
}

// Define a newtype to hook things into the PoolSet machinery.
#[derive(Default)]
pub(crate) struct KeyPresenceTable(HashTable<TableEntry<()>>);

impl Clear for KeyPresenceTable {
    fn clear(&mut self) {
        self.0.clear();
    }
    fn reuse(&self) -> bool {
        self.0.capacity() > 0
    }
}

pub(crate) trait IndexBase {
    /// The type of keys for this index.  Keys can have validity constraints
    /// (e.g. the arity of a slice for `Key = [Value]`). If keys are invalid,
    /// these methods can panic.
    type Key: ?Sized;
    /// Remove any existing entries in the index.
    fn clear(&mut self);
    /// Get the subset corresponding to this key, if there is one.
    fn get_subset<'a, 'b>(
        &'a self,
        key: &'b Self::Key,
    ) -> Option<impl Deref<Target = SubsetRef<'a>>>;
    /// Add the given key and row id to the table.
    fn add_row(&mut self, key: &Self::Key, row: RowId);
    /// Merge the contents of the [`TaggedRowBuffer`] into the table.
    fn merge_rows(&mut self, buf: &TaggedRowBuffer);
    /// Call `f` over the elements of the index.
    fn for_each(&self, f: impl FnMut(&Self::Key, SubsetRef));
    /// The number of keys in the index.
    fn len(&self) -> usize;
}

pub struct ColumnIndex {
    // A specialized index used when we are indexing on a single column.
    table: Pooled<IndexMap<Value, BufferedSubset>>,
    subsets: SubsetBuffer,
}

impl IndexBase for ColumnIndex {
    type Key = Value;
    fn clear(&mut self) {
        for (_, subset) in self.table.drain(..) {
            match subset {
                BufferedSubset::Dense(_) => {}
                BufferedSubset::Sparse(buffered_vec) => {
                    self.subsets.return_vec(buffered_vec);
                }
            }
        }
    }
    fn get_subset<'a>(&'a self, key: &Value) -> Option<impl Deref<Target = SubsetRef<'a>>> {
        self.table.get(key).map(|x| x.as_ref(&self.subsets))
    }
    fn add_row(&mut self, key: &Value, row: RowId) {
        // SAFETY: everything in `table` comes from `subsets`.
        unsafe {
            self.table
                .entry(*key)
                .or_insert_with(BufferedSubset::empty)
                .add_row_sorted(row, &mut self.subsets);
        }
    }
    fn merge_rows(&mut self, buf: &TaggedRowBuffer) {
        for (src_id, key) in buf.iter() {
            debug_assert_eq!(key.len(), 1);
            debug_assert!(!key[0].is_stale());
            self.add_row(&key[0], src_id);
        }
    }
    fn for_each(&self, mut f: impl FnMut(&Self::Key, SubsetRef)) {
        let read_handle = self.subsets.read_handle();
        for (k, v) in self.table.iter() {
            // SAFETY: all of the vectors in `table` come from `subsets`.
            unsafe {
                f(k, *v.as_ref_from_handle(&read_handle));
            }
        }
    }
    fn len(&self) -> usize {
        self.table.len()
    }
}

impl ColumnIndex {
    pub(crate) fn new() -> ColumnIndex {
        with_pool_set(|ps| ColumnIndex {
            table: ps.get(),
            subsets: SubsetBuffer::default(),
        })
    }
}

/// A mapping from keys to subsets of rows.
pub struct TupleIndex {
    // NB: we could store RowBuffers inline and then have indexes reference
    // (u32, RowId) instead of RowId. Trades copying off for indirections.
    subsets: SubsetBuffer,
    keys: RowBuffer,
    table: Pooled<SubsetTable>,
}

impl TupleIndex {
    pub(crate) fn new(key_arity: usize) -> TupleIndex {
        let keys = RowBuffer::new(key_arity);
        with_pool_set(|ps| {
            let table = ps.get();
            TupleIndex {
                keys,
                table,
                subsets: SubsetBuffer::default(),
            }
        })
    }
}

impl IndexBase for TupleIndex {
    type Key = [Value];

    fn clear(&mut self) {
        for entry in self.table.0.drain() {
            match entry.vals {
                BufferedSubset::Dense(_) => {}
                BufferedSubset::Sparse(v) => {
                    self.subsets.return_vec(v);
                }
            }
        }
        self.keys.clear();
    }

    fn get_subset<'a>(&'a self, key: &[Value]) -> Option<impl Deref<Target = SubsetRef<'a>>> {
        let hash = hash_key(key);
        let entry = self.table.0.find(hash, |entry| {
            entry.hash == hash && self.keys.get_row(entry.key) == key
        })?;
        Some(entry.vals.as_ref(&self.subsets))
    }

    fn add_row(&mut self, key: &[Value], row: RowId) {
        let hash = hash_key(key);
        let table_entry = self.table.0.entry(
            hash,
            |entry| entry.hash == hash && self.keys.get_row(entry.key) == key,
            |ent| ent.hash,
        );
        match table_entry {
            hashbrown::hash_table::Entry::Occupied(mut occ) => {
                // SAFETY: everything in `table_entry` comes from `vals`.
                unsafe {
                    occ.get_mut().vals.add_row_sorted(row, &mut self.subsets);
                }
            }
            hashbrown::hash_table::Entry::Vacant(v) => {
                let key_id = self.keys.add_row(key);
                let subset = BufferedSubset::singleton(row);
                v.insert(TableEntry {
                    hash,
                    key: key_id,
                    vals: subset,
                });
            }
        }
    }

    fn merge_rows(&mut self, buf: &TaggedRowBuffer) {
        for (src_id, key) in buf.iter() {
            self.add_row(key, src_id);
        }
    }
    fn for_each(&self, mut f: impl FnMut(&Self::Key, SubsetRef)) {
        // SAFETY: `f` cannot leak references from the callback due to its type.
        let read_handle = self.subsets.read_handle();
        self.table.0.iter().for_each(|entry| {
            let key = self.keys.get_row(entry.key);
            // SAFETY: all of the vectors in `table` come from `subsets`.
            unsafe {
                f(key, *entry.vals.as_ref_from_handle(&read_handle));
            }
        });
    }

    fn len(&self) -> usize {
        self.table.0.len()
    }
}

fn hash_key(key: &[Value]) -> u64 {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    hasher.finish()
}

define_id!(BufferIndex, u32, "an index into a subset buffer");

/// A shared pool of row ids used to store sorted offset vectors with a common
/// lifetime.
///
/// This is used as the backing store for subsets stored in indexes. While this scheme definitely
/// saves some allocations, the primary use for SubsetBuffer is to make deallocation faster: with a
/// standard [`crate::offsets::Subset`] structure stored in the index, dropping requires an O(n)
/// traversal of the index. SubsetBuffer allows deallocation to happen in constant time (given our
/// use of memory pools).
///
/// The uses of BufferedVec and similar in this module are safe, but the API is *not* properly safe
/// as a public API. In particular, passing a BufferedVec to a SubsetBuffer that didn't create it
/// could cause an out-of-bounds read.
struct SubsetBuffer {
    buf: ParallelVecWriter<RowId>,
    free_list: ConcurrentVec<SegQueue<BufferIndex>>,
}

impl Default for SubsetBuffer {
    fn default() -> SubsetBuffer {
        with_pool_set(|ps| {
            let buf: Pooled<Vec<RowId>> = ps.get();
            SubsetBuffer {
                buf: ParallelVecWriter::new(Pooled::into_inner(buf)),
                free_list: ConcurrentVec::with_capacity(4),
            }
        })
    }
}

impl Drop for SubsetBuffer {
    fn drop(&mut self) {
        // Return the underlying vector to the pool.
        Pooled::new(self.buf.take());
    }
}

struct ReadHandle<'a> {
    reader: UnsafeReadAccess<'a, RowId>,
}

impl ReadHandle<'_> {
    /// Get a reference to the underlying subset associated with this vector.
    ///
    /// # Safety
    /// Assumes that the underlying vector is sorted, and that `vec` was returned from the same
    /// SubsetBuffer that this read handle came from.
    unsafe fn make_ref(&self, vec: &BufferedVec) -> SubsetRef<'_> {
        SubsetRef::Sparse(SortedOffsetSlice::new_unchecked(
            self.reader
                .get_unchecked_slice(vec.0.index()..vec.1.index()),
        ))
    }
}

impl SubsetBuffer {
    fn return_vec(&self, vec: BufferedVec) {
        let free_list = self.get_free_list(vec.len());
        free_list.push(vec.0);
    }

    fn read_handle(&self) -> ReadHandle {
        ReadHandle {
            reader: self.buf.unsafe_read_access(),
        }
    }

    fn make_ref<'a>(&'a self, vec: &BufferedVec) -> impl Deref<Target = SubsetRef<'a>> {
        struct Slice<'a, T> {
            _handle: T,
            subset: SubsetRef<'a>,
        }
        impl<'a, T> Deref for Slice<'a, T> {
            type Target = SubsetRef<'a>;
            fn deref(&self) -> &SubsetRef<'a> {
                &self.subset
            }
        }
        let handle = self.buf.read_access();
        let subset = unsafe {
            SortedOffsetSlice::new_unchecked(std::slice::from_raw_parts(
                handle.as_ptr().add(vec.0.index()),
                vec.len(),
            ))
        };
        debug_assert!(subset.inner().iter().all(|x| x.rep() != u32::MAX));
        Slice {
            _handle: handle,
            subset: SubsetRef::Sparse(subset),
        }
    }

    fn new_vec(&self, rows: impl ExactSizeIterator<Item = RowId>) -> BufferedVec {
        let len = rows.len();
        if len == 0 {
            return BufferedVec::default();
        }
        {
            let free_list = self.get_free_list(rows.len());
            if let Some(index) = free_list.pop() {
                let read_handle = self.buf.read_access();
                let mut written = 0;
                let mut cur_ptr =
                    unsafe { (read_handle.as_ptr() as *mut RowId).add(index.index()) };
                for row in rows {
                    assert!(written < len, "ExactSizeIterator lied about its length");
                    unsafe {
                        cur_ptr.write(row);
                        cur_ptr = cur_ptr.add(1);
                    }
                    written += 1;
                }
                assert_eq!(written, len, "ExactSizeIterator lied about its length");
                return BufferedVec(index, BufferIndex::from_usize(index.index() + written));
            }
        }
        // We don't have a previously-used vector in the given size class. Add a new one.
        let mut scratch: Pooled<Vec<RowId>> = with_pool_set(|ps| ps.get());
        scratch.extend(rows);
        assert_eq!(
            scratch.len(),
            len,
            "ExactSizeIterator lied about its length"
        );
        scratch.resize(len.next_power_of_two(), RowId::new(!0));
        let start_index = self.buf.write_contents(scratch.iter().copied());
        let res = BufferedVec(
            BufferIndex::from_usize(start_index),
            BufferIndex::from_usize(start_index + len),
        );
        debug_assert_eq!(self.make_ref(&res)._slice(), &scratch.as_slice()[0..len]);
        res
    }

    /// Push `item` onto the vector.
    ///
    /// # Safety
    /// This method is safe so long as `vec` was returned from this buffer at some point. Aside
    /// from that requirement, the safety of this method relies on the fact that:
    /// * `BufferedVec`s cannot be copied. Hence, methods that take a BufferedVec by value have
    ///   exclusive access to that vector.
    /// * Each `BufferedVec`'s start index identifies a power-of-two-length subslice of the
    ///   underlying buffer _uniquely_ occupied by that vector.
    ///
    /// Together, these requirements ensure that mutable writes to `vec` do not overlap with
    /// another existing borrow of a given cell.
    unsafe fn push_vec(&self, vec: BufferedVec, item: RowId) -> BufferedVec {
        if !vec.is_empty() && !vec.len().is_power_of_two() {
            let read_handle = self.buf.read_access();
            (read_handle.as_ptr() as *mut RowId)
                .add(vec.1.index())
                .write(item);
            return BufferedVec(vec.0, vec.1.inc());
        }
        {
            let free_list = self.get_free_list(vec.len() + 1);
            if let Some(v) = free_list.pop() {
                let read_handle = self.buf.read_access();
                let dst_ptr = read_handle.as_ptr().add(v.index()) as *mut RowId;
                let src_ptr = read_handle.as_ptr().add(vec.0.index());
                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, vec.len());
                dst_ptr.add(vec.len()).write(item);
                let res = BufferedVec(v, BufferIndex::from_usize(v.index() + vec.len() + 1));
                self.return_vec(vec);
                return res;
            }
        } // drop the read guard for free_list
        if vec.is_empty() {
            let start_index = self.buf.write_contents(std::iter::once(item));
            return BufferedVec(
                BufferIndex::from_usize(start_index),
                BufferIndex::from_usize(start_index + 1),
            );
        }

        // We don't have a previously-used vector in the given size class. Add a new one.
        let read_handle = self.buf.unsafe_read_access();
        let mut scratch: Pooled<Vec<RowId>> = with_pool_set(|ps| ps.get());
        scratch.extend((0..(vec.len() + 1).next_power_of_two()).map(|x| {
            use std::cmp::Ordering;
            match x.cmp(&vec.len()) {
                Ordering::Less => *read_handle.get_unchecked(vec.0.index() + x),
                Ordering::Equal => item,
                Ordering::Greater => RowId::new(!0),
            }
        }));
        mem::drop(read_handle);
        let start_index = self.buf.write_contents(scratch.iter().copied());
        BufferedVec(
            BufferIndex::from_usize(start_index),
            BufferIndex::from_usize(start_index + vec.len() + 1),
        )
    }

    /// Get a handle on a free list that can store vectors of the given size.
    ///
    /// The returned object keeps an RCU-style read handle on the buffer, meaning that it can block
    /// resizes of the underlying vector. Callers should avoid keeping these objects around for too
    /// long.
    fn get_free_list(&self, size: usize) -> impl Deref<Target = SegQueue<BufferIndex>> + '_ {
        struct FreeListHandle<T> {
            reader: T,
            index: usize,
        }

        impl<T: Deref<Target = [SegQueue<BufferIndex>]>> Deref for FreeListHandle<T> {
            type Target = SegQueue<BufferIndex>;
            fn deref(&self) -> &SegQueue<BufferIndex> {
                &self.reader[self.index]
            }
        }
        let size_class = size.next_power_of_two().trailing_zeros() as usize;
        let reader = self.free_list.read();
        if size_class < reader.len() {
            return FreeListHandle {
                reader,
                index: size_class,
            };
        }
        mem::drop(reader);
        // There are faster ways to do this.. but it's unlikely we'll ever have more than a few
        // dozen size classes.
        loop {
            let largest_size = self.free_list.push(Default::default());
            if largest_size > size_class {
                break;
            }
        }
        self.get_free_list(size)
    }
}

/// A sorted vector of offsets stored in a [`SubsetBuffer`].
#[derive(Debug)]
pub(crate) struct BufferedVec(BufferIndex, BufferIndex);

impl Default for BufferedVec {
    fn default() -> Self {
        BufferedVec(BufferIndex::new(0), BufferIndex::new(0))
    }
}

impl BufferedVec {
    fn is_empty(&self) -> bool {
        self.0 == self.1
    }
    fn len(&self) -> usize {
        self.1.index() - self.0.index()
    }
}

pub(crate) enum BufferedSubset {
    Dense(OffsetRange),
    Sparse(BufferedVec),
}

impl BufferedSubset {
    /// *Safety:*  callers must ensure that `self` is either dense, or comes from `buf`.
    unsafe fn add_row_sorted(&mut self, row: RowId, buf: &mut SubsetBuffer) {
        match self {
            BufferedSubset::Dense(range) => {
                if range.end == range.start {
                    range.start = row;
                    range.end = row.inc();
                    return;
                }
                if range.end == row {
                    range.end = row.inc();
                    return;
                }
                let mut v = buf.new_vec((range.start.rep()..range.end.rep()).map(RowId::new));
                v = buf.push_vec(v, row);
                *self = BufferedSubset::Sparse(v);
            }
            BufferedSubset::Sparse(vec) => unsafe {
                *vec = buf.push_vec(mem::take(vec), row);
            },
        }
    }

    fn empty() -> Self {
        BufferedSubset::Dense(OffsetRange::new(RowId::new(0), RowId::new(0)))
    }

    fn singleton(row: RowId) -> Self {
        BufferedSubset::Dense(OffsetRange::new(row, row.inc()))
    }

    /// A more finnicky variant of `as_ref` that allows callers to amortize the cost of grabbing a
    /// read handle.
    ///
    /// # Safety
    /// Callers must ensure that the given vector for `Sparse` variants comes from the given
    /// buffer corresponding to the input `ReadHandle`.
    unsafe fn as_ref_from_handle<'a>(
        &self,
        buf: &'a ReadHandle,
    ) -> impl Deref<Target = SubsetRef<'a>> {
        match self {
            BufferedSubset::Dense(range) => {
                WrappedSubset::<VoidWithLifetime<'a>>::Dense(SubsetRef::Dense(*range))
            }
            BufferedSubset::Sparse(vec) => {
                WrappedSubset::<VoidWithLifetime<'a>>::Dense(buf.make_ref(vec))
            }
        }
    }

    fn as_ref<'a>(&self, buf: &'a SubsetBuffer) -> impl Deref<Target = SubsetRef<'a>> {
        match self {
            BufferedSubset::Dense(range) => WrappedSubset::Dense(SubsetRef::Dense(*range)),
            BufferedSubset::Sparse(vec) => WrappedSubset::Sparse(buf.make_ref(vec)),
        }
    }
}

struct VoidWithLifetime<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
    _void: Void,
}

enum Void {}

impl<'a> Deref for VoidWithLifetime<'a> {
    type Target = SubsetRef<'a>;
    fn deref(&self) -> &SubsetRef<'a> {
        match self._void {}
    }
}

enum WrappedSubset<'a, T> {
    Dense(SubsetRef<'a>),
    Sparse(T),
}
impl<'a, T: Deref<Target = SubsetRef<'a>>> Deref for WrappedSubset<'a, T> {
    type Target = SubsetRef<'a>;
    fn deref(&self) -> &SubsetRef<'a> {
        match self {
            WrappedSubset::Dense(s) => s,
            WrappedSubset::Sparse(s) => s.deref(),
        }
    }
}
