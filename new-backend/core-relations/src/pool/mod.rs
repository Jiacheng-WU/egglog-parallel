//! Utilities for pooling object allocations.

use std::{
    cell::{Cell, RefCell},
    fmt,
    hash::{Hash, Hasher},
    mem::{self, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr,
    rc::Rc,
};

use fixedbitset::FixedBitSet;
use hashbrown::HashTable;

use crate::{
    action::Instr,
    common::{HashMap, HashSet, IndexMap, IndexSet, Value},
    free_join::execute::FrameUpdate,
    hash_index::{BufferedSubset, TableEntry},
    offsets::SortedOffsetVector,
    table_spec::Constraint,
    RowId,
};

#[cfg(test)]
mod tests;

/// A trait for types whose allocations can be reused.
pub trait Clear: Default {
    /// Clear the object.
    ///
    /// The end result must be equivalent to `Self::default()`.
    fn clear(&mut self);
    /// Indicate whether or not this object should be reused.
    fn reuse(&self) -> bool {
        true
    }
}

impl<T> Clear for Vec<T> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl<T: Clear> Clear for Rc<T> {
    fn clear(&mut self) {
        Rc::get_mut(self).unwrap().clear()
    }
    fn reuse(&self) -> bool {
        Rc::strong_count(self) == 1 && Rc::weak_count(self) == 0
    }
}

impl<T: Clear> Clone for Pooled<Rc<T>>
where
    Rc<T>: InPoolSet<PoolSet>,
{
    fn clone(&self) -> Self {
        Pooled {
            data: self.data.clone(),
        }
    }
}

impl<T> Clear for HashSet<T> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl<T> Clear for HashTable<T> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl<K, V> Clear for HashMap<K, V> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl<K, V> Clear for IndexMap<K, V> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl<T> Clear for IndexSet<T> {
    fn clear(&mut self) {
        self.clear()
    }
    fn reuse(&self) -> bool {
        self.capacity() > 0
    }
}

impl Clear for FixedBitSet {
    fn clear(&mut self) {
        self.clear();
    }
    fn reuse(&self) -> bool {
        !self.is_empty()
    }
}

/// A shared pool of objects.
pub struct Pool<T> {
    data: Rc<RefCell<Vec<T>>>,
}

impl<T> Clone for Pool<T> {
    fn clone(&self) -> Self {
        Pool {
            data: self.data.clone(),
        }
    }
}

impl<T: Clear> Default for Pool<T> {
    fn default() -> Self {
        Pool {
            data: Default::default(),
        }
    }
}

impl<T: Clear + InPoolSet<PoolSet>> Pool<T> {
    /// Get an empty value of type `T`, potentially reused from the pool.
    pub(crate) fn get(&self) -> Pooled<T> {
        let empty = self.data.borrow_mut().pop().unwrap_or_default();

        Pooled {
            data: ManuallyDrop::new(empty),
        }
    }

    /// Clear the contents of the pool and release any memory associated with it.
    pub(crate) fn clear(&self) {
        let mut data_mut = self.data.borrow_mut();
        data_mut.clear();
        data_mut.shrink_to_fit();
    }
}

/// An owned value of type `T` that can be returned to a memory pool when it is
/// no longer used.
pub struct Pooled<T: Clear + InPoolSet<PoolSet>> {
    data: ManuallyDrop<T>,
}

impl<T: Clear + InPoolSet<PoolSet>> Default for Pooled<T> {
    fn default() -> Self {
        with_pool_set(|ps| ps.get::<T>())
    }
}

impl<T: Clear + fmt::Debug + InPoolSet<PoolSet>> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data: &T = &self.data;
        data.fmt(f)
    }
}
impl<T: Clear + PartialEq + InPoolSet<PoolSet>> PartialEq for Pooled<T> {
    fn eq(&self, other: &Self) -> bool {
        // This form rid of a spuriou clippy warning about unconditional recursion.
        <T as PartialEq>::eq(&self.data, &other.data)
    }
}

impl<T: Clear + InPoolSet<PoolSet> + Eq> Eq for Pooled<T> {}

impl<T: Clear + Hash + InPoolSet<PoolSet>> Hash for Pooled<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state)
    }
}

impl<T: Clear + InPoolSet<PoolSet> + 'static> Pooled<T> {
    /// Clear the contents the wrapped object. If the object cannot be reused,
    /// attempt to fetch another value from the pool.
    ///
    /// This method can be used in concert with `relinquish` to provide a
    /// `clear` operation that hands data back to the pool, and then grabs it
    /// back again if it needs to be reused.
    ///
    /// This pattern is likely only suitable for "temporary" buffers.
    pub(crate) fn refresh(this: &mut Pooled<T>) {
        this.data.clear();
        if this.data.reuse() {
            return;
        }
        let pool = with_pool_set(|ps| ps.get_pool::<T>());
        let Some(mut other) = pool.data.borrow_mut().pop() else {
            return;
        };
        let slot: &mut T = &mut this.data;
        mem::swap(slot, &mut other);
    }

    pub(crate) fn into_inner(this: Pooled<T>) -> T {
        // SAFETY: ownership of `this.data` is transferred to the caller. We
        // will not drop `this` or use it again.
        let inner = unsafe { ptr::read(&this.data) };
        mem::forget(this);
        ManuallyDrop::into_inner(inner)
    }

    pub(crate) fn new(data: T) -> Pooled<T> {
        Pooled {
            data: ManuallyDrop::new(data),
        }
    }
}

impl<T: Clear + Clone + InPoolSet<PoolSet>> Pooled<T> {
    pub(crate) fn cloned(this: &Pooled<T>) -> Pooled<T> {
        let mut res = with_pool_set(|ps| ps.get::<T>());
        res.clone_from(this);
        res
    }
}

impl<T: Clear + InPoolSet<PoolSet>> Drop for Pooled<T> {
    fn drop(&mut self) {
        let reuse = self.data.reuse();
        if !reuse {
            // SAFETY: we own `self.data` and being in the drop method means no
            // one else will access it.
            unsafe { ManuallyDrop::drop(&mut self.data) };
            return;
        }
        self.data.clear();
        let t: &T = &self.data;
        // SAFETY: ownership of `self.data` is transferred to the pool
        with_pool_set(|ps| {
            T::with_pool(ps, |pool| {
                pool.data.borrow_mut().push(unsafe { ptr::read(t) })
            })
        });
    }
}

impl<T: Clear + InPoolSet<PoolSet>> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T: Clear + InPoolSet<PoolSet>> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

/// Helper trait for allowing the trait resolution system to infer the correct
/// pool type during allocation.
pub trait InPoolSet<PoolSet>
where
    Self: Sized + Clear,
{
    fn with_pool<R>(pool_set: &PoolSet, f: impl FnOnce(&Pool<Self>) -> R) -> R;
}

macro_rules! pool_set {
    ($vis:vis $name:ident { $($ident:ident : $ty:ty,)* }) => {
        #[derive(Default)]
        $vis struct $name {
            $(
                $ident: Pool<$ty>,
            )*
        }

        impl $name {
            $vis fn get_pool<T: InPoolSet<Self>>(&self) -> Pool<T> {
                T::with_pool(self, Pool::clone)
            }

            $vis fn get<T: InPoolSet<Self> + Default>(&self) -> Pooled<T> {
                self.get_pool().get()
            }
            $vis fn clear(&self) {
                $( self.$ident.clear(); )*
            }
        }

        $(
            impl InPoolSet<$name> for $ty {
                fn with_pool<R>(pool_set: &$name, f: impl FnOnce(&Pool<Self>) -> R) -> R {
                    f(&pool_set.$ident)
                }
            }
        )*
    }
}

pool_set! {
    pub PoolSet {
        vec_vals: Vec<Value>,
        vec_cell_vals: Vec<Cell<Value>>,
        // TODO: work on scaffolding/DI/etc. so that we can share allocations
        // between vec_vals and shared_vals.
        rows: Vec<RowId>,
        offset_vec: SortedOffsetVector,
        column_index: IndexMap<Value, BufferedSubset>,
        constraints: Vec<Constraint>,
        bitsets: FixedBitSet,
        instrs: Vec<Instr>,
        frame_updates: FrameUpdate,
        frame_update_vecs: Vec<Pooled<FrameUpdate>>,
        tuple_indexes: HashTable<TableEntry<BufferedSubset>>,
    }
}

/// Run `f` on the thread-local [`PoolSet`].
pub(crate) fn with_pool_set<R>(f: impl FnOnce(&PoolSet) -> R) -> R {
    POOL_SET.with(|pool_set| f(pool_set))
}

thread_local! {
    /// A thread-local pool set. All pooled allocations land back in the local thread.
    ///
    /// We don't drop this PoolSet because it does not contain any resources
    /// that need to be released, other than memory (which will be reclaimed
    /// when the process exits, right after drop runs).
    ///
    /// For large egraphs, this be a big runtime win. The main egglog binary
    /// avoids dropping the egraph for the same reason.
    static POOL_SET: ManuallyDrop<PoolSet> = Default::default();
}
