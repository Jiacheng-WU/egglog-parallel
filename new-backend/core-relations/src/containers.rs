//! Support for containers
//!
//! Containers behave a lot like primitives in egglog. They are implemented differently because
//! their ids share a space with other Ids in the egraph and as a result, their ids need to be
//! sparse.
//!
//! This is a relatively "eagler" implementation of containers, reflecting egglog's current
//! semantics. One could imagine a variant of containers in which they behave more like egglog
//! functions than primitives.

use std::{
    any::{Any, TypeId},
    hash::{Hash, Hasher},
    ops::Deref,
};

use numeric_id::{define_id, DenseIdMap, NumericId};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rustc_hash::FxHasher;

use crate::{
    common::{DashMap, IndexSet, InternTable},
    table_spec::Rewriter,
    CounterId, ExecutionState, Value,
};

define_id!(pub ContainerId, u32, "an identifier for containers");

pub type MergeFn = dyn Fn(&mut ExecutionState, Value, Value) -> Value + Send + Sync;

#[derive(Default)]
pub struct Containers {
    container_ids: InternTable<TypeId, ContainerId>,
    data: DenseIdMap<ContainerId, Box<dyn DynamicContainerEnv + Send + Sync>>,
}

impl Containers {
    pub fn new() -> Self {
        Default::default()
    }

    fn get<C: Container>(&self) -> Option<&ContainerEnv<C>> {
        let id = self.container_ids.intern(&TypeId::of::<C>());
        let res = self.data.get(id)?.as_any();
        Some(res.downcast_ref::<ContainerEnv<C>>().unwrap())
    }

    /// Iterate over the containers of the given type.
    pub fn for_each<C: Container>(&self, mut f: impl FnMut(&C, Value)) {
        let Some(env) = self.get::<C>() else {
            return;
        };
        for ent in env.to_id.iter() {
            f(ent.key(), *ent.value());
        }
    }

    /// Get the container associated with the value `val` in the database. The caller must know the
    /// type of the container.
    ///
    /// The return type of this function may contain lock guards. Attempts to modify the contents
    /// of the containers database may deadlock if the given guard has not been dropped.
    pub fn get_val<C: Container>(&self, val: Value) -> Option<impl Deref<Target = C> + '_> {
        self.get::<C>()?.get_container(val)
    }

    pub fn register_val<C: Container>(
        &self,
        container: C,
        exec_state: &mut ExecutionState,
    ) -> Value {
        let env = self
            .get::<C>()
            .expect("must register container type before registering a value");
        env.get_or_insert(&container, exec_state)
    }

    /// Apply the given rewrite rule to the contents of each container.
    pub fn rewrite_all(
        &mut self,
        rewriter: &dyn Rewriter,
        exec_state: &mut ExecutionState,
    ) -> bool {
        if do_parallel() {
            self.data
                .iter_mut()
                .zip(std::iter::repeat_with(|| exec_state.new_handle()))
                .par_bridge()
                .map(|((_, env), mut exec_state)| env.apply_rewrite(rewriter, &mut exec_state))
                .max()
                .unwrap_or(false)
        } else {
            let mut changed = false;
            for (_, env) in self.data.iter_mut() {
                changed |= env.apply_rewrite(rewriter, exec_state);
            }
            changed
        }
    }

    /// Add a new container type to the given [`Containers`] instance.
    ///
    /// Container types need a meaans of generating fresh ids (`id_counter`) along with a means of
    /// merging conflicting ids (`merge_fn`).
    pub fn register_type<C: Container>(
        &mut self,
        id_counter: CounterId,
        merge_fn: impl Fn(&mut ExecutionState, Value, Value) -> Value + Send + Sync + 'static,
    ) -> ContainerId {
        let id = self.container_ids.intern(&TypeId::of::<C>());
        self.data.get_or_insert(id, || {
            Box::new(ContainerEnv::<C>::new(Box::new(merge_fn), id_counter))
        });
        id
    }
}

/// A trait implemented by container types.
///
/// Containers behave a lot like primitives, but they include extra trait methods to support
/// rebuilding of container contents and merging containers that become equal after a rewrite pass
/// as taken place.
pub trait Container: Hash + Eq + Clone + Send + Sync + 'static {
    /// Rewrite an additional container in place according the the given [`Rewriter`].
    ///
    /// If this method returns `false` then the container must not have been modified (i.e. it must
    /// hash to the same value, and compare equal to a copy of itself before the call).
    fn rewrite_contents(&mut self, rewriter: &dyn Rewriter) -> bool;

    /// Iterate over the contents of the container.
    ///
    /// Note that containers can be more structured than just a sequence of values. This iterator
    /// is used to populate an index that in turn is used to speed up rewrites. If a value in the
    /// container is eligible for a rewrite and it is not mentioned by this iterator, the outer
    /// [`ContainerEnv`] may skip rewriting this container.
    fn iter(&self) -> impl Iterator<Item = Value> + '_;
}

pub trait DynamicContainerEnv: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn apply_rewrite(&mut self, rewriter: &dyn Rewriter, exec_state: &mut ExecutionState) -> bool;
}

fn hash_container(container: &impl Container) -> usize {
    let mut hasher = FxHasher::default();
    container.hash(&mut hasher);
    hasher.finish() as usize
}

struct ContainerEnv<C> {
    merge_fn: Box<MergeFn>,
    counter: CounterId,
    to_id: DashMap<C, Value>,
    to_container: DashMap<Value, (usize /* hash code */, usize /* map */)>,
    /// Map from a Value to the set of ids of containers that contain that value.
    val_index: DashMap<Value, IndexSet<Value>>,
}

impl<C: Container> DynamicContainerEnv for ContainerEnv<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn apply_rewrite(&mut self, rewriter: &dyn Rewriter, exec_state: &mut ExecutionState) -> bool {
        self.apply_rewrite_nonincremental(rewriter, exec_state)
    }
}

impl<C: Container> ContainerEnv<C> {
    pub fn new(merge_fn: Box<MergeFn>, counter: CounterId) -> Self {
        Self {
            merge_fn,
            counter,
            to_id: DashMap::default(),
            to_container: DashMap::default(),
            val_index: DashMap::default(),
        }
    }
    fn get_or_insert(&self, container: &C, exec_state: &mut ExecutionState) -> Value {
        match self.to_id.get(container) {
            Some(value) => *value,
            None => {
                let value = Value::from_usize(exec_state.inc_counter(self.counter));
                self.to_id.insert(container.clone(), value);
                let target_map = self.to_id.determine_map(container);
                self.to_container
                    .insert(value, (hash_container(container), target_map));
                for val in container.iter() {
                    self.val_index.entry(val).or_default().insert(value);
                }
                value
            }
        }
    }

    fn insert_owned(&self, container: C, value: Value, exec_state: &mut ExecutionState) {
        let hc = hash_container(&container);
        let target_map = self.to_id.determine_map(&container);
        match self.to_id.entry(container) {
            dashmap::Entry::Occupied(mut occ) => {
                let result = (self.merge_fn)(exec_state, *occ.get(), value);
                let old_val = *occ.get();
                if result != old_val {
                    self.to_container.remove(&old_val);
                    self.to_container.insert(result, (hc, target_map));
                    *occ.get_mut() = result;
                    for val in occ.key().iter() {
                        let mut index = self.val_index.entry(val).or_default();
                        index.swap_remove(&old_val);
                        index.insert(result);
                    }
                }
            }
            dashmap::Entry::Vacant(vacant_entry) => {
                self.to_container.insert(value, (hc, target_map));
                for val in vacant_entry.key().iter() {
                    self.val_index.entry(val).or_default().insert(value);
                }
                vacant_entry.insert(value);
            }
        }
    }
    fn apply_rewrite_nonincremental(
        &mut self,
        rewriter: &dyn Rewriter,
        exec_state: &mut ExecutionState,
    ) -> bool {
        if do_parallel() {
            todo!()
        } else {
            let mut changed = false;
            let mut to_reinsert = Vec::new();
            let shards = self.to_id.shards_mut();
            for shard in shards.iter_mut() {
                let shard = shard.get_mut();
                // SAFETY: the iterator does not outlive `shard`.
                for bucket in unsafe { shard.iter() } {
                    // SAFETY: the bucket is valid; we just got it from the iterator.
                    let (container, val) = unsafe { bucket.as_mut() };
                    let old_val = *val.get();
                    let new_val = rewriter.rewrite_val(old_val);
                    let container_changed = container.rewrite_contents(rewriter);
                    if !container_changed && new_val == old_val {
                        // Nothing changed about this entry. Leave it in place.
                        continue;
                    }
                    changed = true;
                    if container_changed {
                        // The container changed. Remove both map entries then reinsert.
                        // SAFETY: This is a valid bucket. Furthermore, iterators remain valid if
                        // buckets they have already yielded have been removed.
                        let ((container, _), _) = unsafe { shard.remove(bucket) };
                        self.to_container.remove(&old_val);
                        to_reinsert.push((container, new_val));
                    } else {
                        // Just the value changed. Leave the container in place.
                        *val.get_mut() = new_val;
                        let prev = self.to_container.remove(&old_val).unwrap().1;
                        self.to_container.insert(new_val, prev);
                    }
                }
            }
            for (container, val) in to_reinsert {
                self.insert_owned(container, val, exec_state);
            }
            changed
        }
    }

    fn get_container(&self, value: Value) -> Option<impl Deref<Target = C> + '_> {
        let (hc, target_map) = *self.to_container.get(&value)?;
        let shard = &self.to_id.shards()[target_map];
        let read_guard = shard.read();
        let val_ptr: *const (C, _) = shard
            .read()
            .find(hc as u64, |(_, v)| *v.get() == value)?
            .as_ptr();
        struct ValueDeref<'a, T, Guard> {
            _guard: Guard,
            data: &'a T,
        }

        impl<T, Guard> Deref for ValueDeref<'_, T, Guard> {
            type Target = T;

            fn deref(&self) -> &T {
                self.data
            }
        }

        Some(ValueDeref {
            _guard: read_guard,
            // SAFETY: the value will remain valid for as long as `read_guard` is in scope.
            data: unsafe {
                let unwrapped: &(C, _) = &*val_ptr;
                &unwrapped.0
            },
        })
    }
}

fn do_parallel() -> bool {
    let todo_revert = 1;
    false
    // #[cfg(test)]
    // {
    //     use rand::Rng;
    //     rand::thread_rng().gen_bool(0.5)
    // }

    // #[cfg(not(test))]
    // {
    //     rayon::current_num_threads() > 1
    // }
}

type TodoIncrementalRebuildling = ();
type TodoParallelism = ();
