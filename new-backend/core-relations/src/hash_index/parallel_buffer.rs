//! A variant of SubsetBuffer that supports concurrent insertions.

use std::{mem, ops::Deref};

use concurrency::{parallel_writer::UnsafeReadAccess, ParallelVecWriter};
use numeric_id::NumericId;

use crate::{
    common::DashMap, offsets::SortedOffsetSlice, pool::with_pool_set, Pooled, RowId, SubsetRef,
};

use super::{BufferIndex, BufferedVec, SubsetBuffer};

#[derive(Default)]
pub(super) struct FreeList {
    data: DashMap<usize, Vec<BufferIndex>>,
}

impl FreeList {
    pub(super) fn with_size_class<R>(
        &self,
        size: usize,
        f: impl FnOnce(&mut Vec<BufferIndex>) -> R,
    ) -> R {
        let size_class = size.next_power_of_two();
        let mut guard = self.data.entry(size_class).or_default();
        f(&mut guard)
    }
}

/// A buffer for sorted vectors of [`RowId`]s that supports parallel writes.
pub(super) struct ParallelSubsetBuffer {
    buf: ParallelVecWriter<RowId>,
    free_list: FreeList,
}

struct ReadHandle<'a> {
    reader: UnsafeReadAccess<'a, RowId>,
}

impl ReadHandle<'_> {
    /// Get a reference to the underlying subset associated with this vector.
    ///
    /// # Safety
    /// Assumes that the underlying vector is sorted, and that `vec` was returned from the same
    /// ParallelSubsetBuffer that this read handle came from.
    unsafe fn make_ref(&self, vec: &BufferedVec) -> SubsetRef<'_> {
        SubsetRef::Sparse(SortedOffsetSlice::new_unchecked(
            self.reader
                .get_unchecked_slice(vec.0.index()..vec.1.index()),
        ))
    }
}

impl ParallelSubsetBuffer {
    pub(super) fn from_serial(subsets: SubsetBuffer) -> ParallelSubsetBuffer {
        ParallelSubsetBuffer {
            buf: ParallelVecWriter::new(Pooled::into_inner(subsets.buf)),
            free_list: subsets.free_list,
        }
    }
    pub(super) fn finish(self) -> SubsetBuffer {
        SubsetBuffer {
            buf: Pooled::new(self.buf.finish()),
            free_list: self.free_list,
        }
    }
    fn return_vec(&self, vec: BufferedVec) {
        self.free_list.with_size_class(vec.len(), |v| v.push(vec.0));
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

    pub(super) fn new_vec(&self, rows: impl ExactSizeIterator<Item = RowId>) -> BufferedVec {
        let len = rows.len();
        if len == 0 {
            return BufferedVec::default();
        }
        {
            if let Some(index) = self.free_list.with_size_class(rows.len(), Vec::pop) {
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
    pub(super) unsafe fn push_vec(&self, vec: BufferedVec, item: RowId) -> BufferedVec {
        if !vec.is_empty() && !vec.len().is_power_of_two() {
            let read_handle = self.buf.read_access();
            (read_handle.as_ptr() as *mut RowId)
                .add(vec.1.index())
                .write(item);
            return BufferedVec(vec.0, vec.1.inc());
        }
        if let Some(v) = self.free_list.with_size_class(vec.len() + 1, Vec::pop) {
            let read_handle = self.buf.read_access();
            let dst_ptr = read_handle.as_ptr().add(v.index()) as *mut RowId;
            let src_ptr = read_handle.as_ptr().add(vec.0.index());
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, vec.len());
            dst_ptr.add(vec.len()).write(item);
            let res = BufferedVec(v, BufferIndex::from_usize(v.index() + vec.len() + 1));
            self.return_vec(vec);
            return res;
        }
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
}
