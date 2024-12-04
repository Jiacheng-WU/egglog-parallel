//! Managing a universe of containers.

use std::{hash::BuildHasherDefault, ops::Deref, sync::Arc};

use core_relations::Value;
use dashmap::DashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{Container, UnionFindHandle};

type HashMap<K, V> = DashMap<K, V, BuildHasherDefault<rustc_hash::FxHasher>>;
type IndexSet<T> = indexmap::IndexSet<T, BuildHasherDefault<rustc_hash::FxHasher>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Id(Value);

// The implementation here serves as a baseline for supporting Containers with passable
// multithreaded performance. There is a lot of room for improvement here by simply batching
// pending updates on a per-shard basis and only grabbing one lock per update.

// TODO/notes:
// 1. Probably want to start with dashmap everywhere for parallelism.
// 2. Need to figure out how to scaffold things wrt egglog. Side-channels into a target table?
// 3. Probably do that in the egglog crate. Maybe not? Maybe we can do it here.

/// A structre managing a universe of containers of a particular type `C`.
pub struct ContainerEnv<C> {
    to_id: HashMap<Arc<C>, Id>,
    to_container: HashMap<Id, Arc<C>>,
    id_index: HashMap<Value, IndexSet<Id>>,
}

impl<C: Container> Default for ContainerEnv<C> {
    fn default() -> Self {
        Self {
            to_id: Default::default(),
            to_container: Default::default(),
            id_index: Default::default(),
        }
    }
}

impl<C: Container> ContainerEnv<C> {
    pub(crate) fn get_id(&mut self, container: C, id_generator: &impl UnionFindHandle) -> Value {
        self.get_id_internal(container, None, id_generator).0
    }

    fn get_id_internal(
        &self,
        container: C,
        suggested_id: Option<Id>,
        id_generator: &impl UnionFindHandle,
    ) -> Id {
        if let Some(id) = self.to_id.get(&container) {
            let id = id.0;
            if let Some(Id(sugg)) = suggested_id {
                id_generator.union(id, sugg);
            }
            Id(id)
        } else {
            let container = Arc::new(container);
            let id = suggested_id.unwrap_or_else(|| Id(id_generator.generate_id()));
            for val in container.contents() {
                self.id_index.entry(val).or_default().insert(id);
            }
            self.to_id.insert(container.clone(), id);
            self.to_container.insert(id, container);
            id
        }
    }
    pub(crate) fn get_container(&self, id: Value) -> Option<impl Deref<Target = C> + '_> {
        Some(self.to_container.get(&Id(id))?.map(|c| c.deref()))
    }

    fn rebuild_incremental(&self, stale: &[Value], id_generator: impl UnionFindHandle) {
        let rebuild_ids_for = |this: &ContainerEnv<C>, stale_val: &Value, id_gen: &_| {
            for id in this
                .id_index
                .remove(stale_val)
                .into_iter()
                .flat_map(|(_, ids)| ids.into_iter())
            {
                this.rebuild_id(id, id_gen);
            }
        };

        if do_parallel(stale.len()) {
            rayon::in_place_scope(|scope| {
                for chunk in stale.chunks(1000) {
                    let handle = id_generator.new_handle();
                    scope.spawn(move |_| {
                        for stale_val in chunk {
                            rebuild_ids_for(self, stale_val, &handle);
                        }
                    });
                }
            });
        } else {
            for stale_val in stale {
                rebuild_ids_for(self, stale_val, &id_generator);
            }
        }
    }

    fn rebuild_id(&self, id: Id, id_generator: &impl UnionFindHandle) {
        let Some((_, container)) = self.to_container.remove(&id) else {
            // We already rebuilt this container.
            return;
        };
        let _was = self.to_id.remove(&container).unwrap();
        let mut container = Arc::try_unwrap(container).ok().unwrap();
        container.rebuild(|val| id_generator.find(val));
        // Reinsert the new value.
        self.get_id_internal(container, Some(id), id_generator);
    }

    fn rebuild_full(&self, id_generator: impl UnionFindHandle) {
        let ids = Vec::from_iter(self.to_id.iter().map(|refmulti| *refmulti.value()));
        if do_parallel(ids.len()) {
            rayon::in_place_scope(|scope| {
                for chunk in ids.chunks(1000) {
                    let handle = id_generator.new_handle();
                    scope.spawn(move |_| {
                        for id in chunk {
                            self.rebuild_id(*id, &handle);
                        }
                    });
                }
            });
        } else {
            for id in ids {
                self.rebuild_id(id, &id_generator);
            }
        }
    }
}

fn do_parallel(_workload_size: usize) -> bool {
    // Run the parallel workload 50% of the time regardless of workload size, to
    // ensure we get reasonable coverage of both paths.
    #[cfg(test)]
    {
        use rand::Rng;
        rand::thread_rng().gen_bool(0.5)
    }
    #[cfg(not(test))]
    {
        rayon::current_num_threads() > 1 && _workload_size > 10_000
    }
}
