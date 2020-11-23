use differential_dataflow::difference::Semigroup;
use differential_dataflow::Collection;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use timely::dataflow::scopes::Scope;
use timely::Data;

mod core;
mod semigroup_wrapper;

pub use crate::core::*;
use semigroup_wrapper::*;

pub type ReadMapRef<D, R> = ReadRef<HashMap<D, R>, D, R>;

pub trait CreateHashedOutput<D, R> {
    fn create_hashed_output(&self) -> ReadRef<HashMap<D, R>, D, R>;
    fn create_output(&self) -> ReadMapRef<D, R> {
        self.create_hashed_output()
    }
}
impl<G: Scope<Timestamp = usize>, D: Data + Eq + Hash, R: Default + Semigroup>
    CreateHashedOutput<D, R> for Collection<G, D, R>
{
    fn create_hashed_output(&self) -> ReadRef<HashMap<D, R>, D, R> {
        self.create_updater(|data, d, r| apply_hash_update(data, d, SG(r)))
    }
}

pub struct OrderedRef<D, R>(ReadRef<BTreeMap<D, R>, D, R>);
pub struct OrderedRefRef<'b, D, R>(ReadRefRef<'b, BTreeMap<D, R>, D, R>);
impl<D, R> OrderedRef<D, R> {
    pub fn read<'b>(&'b self, context: &'b ContextOutput) -> OrderedRefRef<'b, D, R> {
        OrderedRefRef(self.0.read(context))
    }
}
impl<'b, D, R> OrderedRefRef<'b, D, R> {
    pub fn min<'c>(&'c self) -> Option<&'c D> {
        self.0.iter().next().map(|(x, _)| x)
    }
    pub fn max<'c>(&'c self) -> Option<&'c D> {
        self.0.iter().next_back().map(|(x, _)| x)
    }
}

pub trait CreateOrderedOutput<D, R> {
    fn create_btree_output(&self) -> ReadRef<BTreeMap<D, R>, D, R>;
    fn create_ordered_output(&self) -> OrderedRef<D, R> {
        OrderedRef(self.create_btree_output())
    }
}
impl<G: Scope<Timestamp = usize>, D: Data + Ord, R: Default + Semigroup> CreateOrderedOutput<D, R>
    for Collection<G, D, R>
{
    fn create_btree_output(&self) -> ReadRef<BTreeMap<D, R>, D, R> {
        self.create_updater(|data, d, r| apply_btree_update(data, d, SG(r)))
    }
}

pub trait CreateCountOutput<D, R> {
    fn create_count_output(&self) -> ReadRef<R, D, R>;
}
impl<G: Scope<Timestamp = usize>, D: Data, R: Default + Semigroup> CreateCountOutput<D, R>
    for Collection<G, D, R>
{
    fn create_count_output(&self) -> ReadRef<R, D, R> {
        self.create_updater(|data, _, r| *data += &r)
    }
}

pub trait CreateMapOutput<K, V, R> {
    fn create_map_output(&self) -> ReadRef<HashMap<K, HashMap<V, R>>, (K, V), R>;
}
impl<
        G: Scope<Timestamp = usize>,
        K: Data + Eq + Hash,
        V: Data + Eq + Hash,
        R: Default + Semigroup,
    > CreateMapOutput<K, V, R> for Collection<G, (K, V), R>
{
    fn create_map_output(&self) -> ReadRef<HashMap<K, HashMap<V, R>>, (K, V), R> {
        self.create_updater(|data, d, r| apply_map_update(data, d, SG(r)))
    }
}

impl<D: Clone + Ord + Debug, R: Semigroup> ReadMapRef<D, R> {
    pub fn feedback(self: &Self, context: &mut Context, input: &InputSession<D, R>) {
        for (k, v) in self.read(&context.get_output()).iter() {
            input.update(&context.get_input(), k.clone(), v.clone());
        }
    }
}
