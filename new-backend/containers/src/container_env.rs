//! Managing a universe of containers.

use std::{hash::BuildHasherDefault, sync::Arc};

use core_relations::Value;

use crate::{Container, IdGenerator};

type HashMap<K, V> = hashbrown::HashMap<K, V, BuildHasherDefault<rustc_hash::FxHasher>>;
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

impl<C> Default for ContainerEnv<C> {
    fn default() -> Self {
        Self {
            to_id: Default::default(),
            to_container: Default::default(),
            id_index: Default::default(),
        }
    }
}

impl<C: Container> ContainerEnv<C> {
    pub(crate) fn get_id(&mut self, container: C, id_generator: &impl IdGenerator) -> Value {
        self.get_id_internal(container, None, id_generator).0
    }

    fn get_id_internal(
        &mut self,
        container: C,
        suggested_id: Option<Id>,
        id_generator: &impl IdGenerator,
    ) -> Id {
        if let Some(Id(id)) = self.to_id.get(&container) {
            if let Some(Id(sugg)) = suggested_id {
                id_generator.union(*id, sugg);
            }
            Id(*id)
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
    pub(crate) fn get_container(&self, id: Value) -> Option<&C> {
        Some(self.to_container.get(&Id(id))?)
    }

    fn rebuild_incremental(&mut self, stale: &[Value], id_generator: &impl IdGenerator) {
        for stale_val in stale {
            for id in self.id_index.remove(stale_val).into_iter().flatten() {
                self.rebuild_id(id, id_generator);
            }
        }
    }

    fn rebuild_id(&mut self, id: Id, id_generator: &impl IdGenerator) {
        let Some(container) = self.to_container.remove(&id) else {
            // We already rebuilt this container.
            return;
        };
        let _was = self.to_id.remove(&container).unwrap();
        let mut container = Arc::try_unwrap(container).ok().unwrap();
        container.rebuild(|val| id_generator.find(val));
        // Reinsert the new value.
        self.get_id_internal(container, Some(id), id_generator);
    }

    fn rebuild_full(&mut self, id_generator: &impl IdGenerator) {
        let ids = Vec::from_iter(self.to_id.values().copied());
        for id in ids {
            self.rebuild_id(id, id_generator);
        }
    }
}
