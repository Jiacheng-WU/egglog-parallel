//! Utilities for memoizing hashes of values.
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone, Copy)]
pub(crate) struct WithHash<C> {
    hash: u64,
    data: C,
}

impl<C> WithHash<C> {
    pub(crate) fn into_inner(self) -> C {
        self.data
    }
}

impl<C> Deref for WithHash<C> {
    type Target = C;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<C> DerefMut for WithHash<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<C: Default + Hash> Default for WithHash<C> {
    fn default() -> Self {
        C::default().into()
    }
}

impl<C: Hash> From<C> for WithHash<C> {
    fn from(data: C) -> Self {
        let hash = {
            use rustc_hash::FxHasher;
            use std::hash::Hasher;
            let mut hasher = FxHasher::default();
            data.hash(&mut hasher);
            hasher.finish()
        };
        Self { hash, data }
    }
}

impl<C> Hash for WithHash<C> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl<C: PartialEq> PartialEq for WithHash<C> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.data == other.data
    }
}

impl<C: Eq> Eq for WithHash<C> {}
