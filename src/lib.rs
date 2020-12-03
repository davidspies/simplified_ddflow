use differential_dataflow::difference::Semigroup;
use differential_dataflow::Collection;
use std::collections::{hash_map, BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use timely::dataflow::scopes::Scope;
use timely::Data;

mod core;
mod semigroup_wrapper;

pub use crate::core::*;
use semigroup_wrapper::*;

pub type ReadMapRef<D, R = isize> = ReadRef<HashMap<D, R>, D, R>;

pub trait CreateHashedOutput<D, R = isize> {
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

pub struct OrderedRef<D, R = isize>(ReadRef<BTreeMap<D, R>, D, R>);
pub struct OrderedRefRef<'b, D, R = isize>(ReadRefRef<'b, BTreeMap<D, R>, D, R>);
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

pub trait CreateOrderedOutput<D, R = isize> {
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

pub trait CreateCountOutput<D, R = isize> {
    fn create_count_output(&self) -> ReadRef<R, D, R>;
}
impl<G: Scope<Timestamp = usize>, D: Data, R: Default + Semigroup> CreateCountOutput<D, R>
    for Collection<G, D, R>
{
    fn create_count_output(&self) -> ReadRef<R, D, R> {
        self.create_updater(|data, _, r| *data += &r)
    }
}

type UndefaultedMap<K, V, R = isize> = ReadRef<HashMap<K, HashMap<V, R>>, (K, V), R>;

pub trait CreateMapOutput<K, V, R = isize> {
    fn create_map_map_output(&self) -> ReadMapMapRef<K, V, R>;
}
impl<
        G: Scope<Timestamp = usize>,
        K: Data + Eq + Hash,
        V: Data + Eq + Hash,
        R: Default + Semigroup,
    > CreateMapOutput<K, V, R> for Collection<G, (K, V), R>
{
    fn create_map_map_output(&self) -> ReadMapMapRef<K, V, R> {
        self.create_updater(|data, d, r| apply_map_update(data, d, SG(r)))
            .with_default()
    }
}

pub trait CreateSingletonMapOutput<K, V> {
    fn create_singleton_map_output(&self) -> SingletonMap<K, V>;
}
impl<G: Scope<Timestamp = usize>, K: Data + Eq + Hash, V: Data + Eq + Hash>
    CreateSingletonMapOutput<K, V> for Collection<G, (K, V)>
{
    fn create_singleton_map_output(&self) -> SingletonMap<K, V> {
        self.create_updater(|data, d, r| apply_map_update(data, d, SG(r)))
            .singleton_map()
    }
}

pub struct ReadMapMapRef<K, V, R = isize>(UndefaultedMap<K, V, R>);

impl<K, V, R> UndefaultedMap<K, V, R> {
    fn with_default(self) -> ReadMapMapRef<K, V, R> {
        ReadMapMapRef(self)
    }
}

impl<K, V, R> ReadMapMapRef<K, V, R> {
    pub fn read<'a>(&'a self, context: &'a ContextOutput) -> DefaultedRef<'a, K, V, R> {
        DefaultedRef(self.0.read(context))
    }
}

pub struct DefaultedRef<'a, K, V, R = isize>(ReadRefRef<'a, HashMap<K, HashMap<V, R>>, (K, V), R>);

pub enum MaybeBorrowed<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> Deref for MaybeBorrowed<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeBorrowed::Owned(x) => &x,
            MaybeBorrowed::Borrowed(x) => x,
        }
    }
}

impl<K: Eq + Hash, V, R> DefaultedRef<'_, K, V, R> {
    pub fn get(&self, k: &K) -> MaybeBorrowed<HashMap<V, R>> {
        match self.0.get(k) {
            None => MaybeBorrowed::Owned(HashMap::new()),
            Some(v) => MaybeBorrowed::Borrowed(v),
        }
    }
}

impl<'a, K, V, R> IntoIterator for &'a DefaultedRef<'_, K, V, R> {
    type Item = (&'a K, &'a HashMap<V, R>);
    type IntoIter = hash_map::Iter<'a, K, HashMap<V, R>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

pub struct SingletonMap<K, V>(UndefaultedMap<K, V>);

impl<K, V> UndefaultedMap<K, V> {
    fn singleton_map(self) -> SingletonMap<K, V> {
        SingletonMap(self)
    }
}

impl<K, V> SingletonMap<K, V> {
    pub fn read<'a>(&'a self, context: &'a ContextOutput) -> SingletonMapRef<'a, K, V> {
        SingletonMapRef(self.0.read(context))
    }
}

impl<K: Eq + Hash, V> SingletonMapRef<'_, K, V> {
    pub fn get(&self, k: &K) -> Option<&V> {
        self.0.get(k).map(get_singleton)
    }
}

fn get_singleton<K>(vmap: &HashMap<K, isize>) -> &K {
    let mut iter = vmap.iter();
    match iter.next() {
        Some((x, &r)) => {
            if iter.next().is_some() {
                panic!("Too many elements")
            } else if r != 1 {
                panic!("Bad count")
            } else {
                x
            }
        }
        None => panic!("Empty map"),
    }
}

impl<'a, K, V> IntoIterator for &'a SingletonMapRef<'_, K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = SingletonMapIter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        SingletonMapIter(self.0.iter())
    }
}

pub struct SingletonMapIter<'a, K, V>(hash_map::Iter<'a, K, HashMap<V, isize>>);

impl<'a, K, V> Iterator for SingletonMapIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, vmap)| (k, get_singleton(vmap)))
    }
}

pub struct SingletonMapRef<'a, K, V>(ReadRefRef<'a, HashMap<K, HashMap<V, isize>>, (K, V)>);

pub type ReadOrderedMapRef<K, V, R = isize> = ReadRef<BTreeMap<K, HashMap<V, R>>, (K, V), R>;

pub trait CreateOrderedMapOutput<K, V, R = isize> {
    fn create_ordered_map_output(&self) -> ReadOrderedMapRef<K, V, R>;
}

impl<G: Scope<Timestamp = usize>, K: Data + Ord, V: Data + Eq + Hash, R: Default + Semigroup>
    CreateOrderedMapOutput<K, V, R> for Collection<G, (K, V), R>
{
    fn create_ordered_map_output(&self) -> ReadOrderedMapRef<K, V, R> {
        self.create_updater(|data, d, r| apply_btree_map_update(data, d, SG(r)))
    }
}

impl<D: Clone + Ord + Debug, R: Semigroup> ReadMapRef<D, R> {
    pub fn feedback(&self, context: &mut Context, input: &InputSession<D, R>) {
        for (k, v) in self.read(&context.get_output()).iter() {
            input.update(&context.get_input(), k.clone(), v.clone());
        }
    }
}

pub struct Distinct<D>(ReadMapRef<D>);

impl<D> ReadMapRef<D> {
    pub fn assert_distinct(self) -> Distinct<D> {
        Distinct(self)
    }
}

pub struct DistinctRef<'a, D>(ReadRefRef<'a, HashMap<D, isize>, D>);

impl<D: Eq + Hash> DistinctRef<'_, D> {
    pub fn contains(&self, k: &D) -> bool {
        match self.0.get(k) {
            None => false,
            Some(&r) => {
                assert_eq!(r, 1);
                true
            }
        }
    }
}

impl<'a, D> IntoIterator for &'a DistinctRef<'_, D> {
    type Item = &'a D;
    type IntoIter = DistinctIter<'a, D>;

    fn into_iter(self) -> Self::IntoIter {
        DistinctIter(self.0.iter())
    }
}

pub struct DistinctIter<'a, D>(hash_map::Iter<'a, D, isize>);

impl<'a, D> Iterator for DistinctIter<'a, D> {
    type Item = &'a D;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, &v)| {
            assert_eq!(v, 1);
            k
        })
    }
}

pub trait Has1 {
    fn assert_one(self);
}

impl Has1 for isize {
    fn assert_one(self) {
        assert_eq!(self, 1)
    }
}

impl<'a> Has1 for &'a isize {
    fn assert_one(self) {
        assert_eq!(*self, 1)
    }
}

pub trait Assert1s<K, R: Has1>: Iterator<Item = (K, R)> {
    type Output: Iterator<Item = K>;

    fn assert_ones(self) -> Self::Output;
}

pub struct Assert1sImpl<I: Iterator<Item = (K, R)>, K, R>(I);

impl<I: Iterator<Item = (K, R)>, K, R: Has1> Iterator for Assert1sImpl<I, K, R> {
    type Item = K;

    fn next(&mut self) -> Option<K> {
        self.0.next().map(|(k, v)| {
            v.assert_one();
            k
        })
    }
}

impl<I: Iterator<Item = (K, R)>, K, R: Has1> Assert1s<K, R> for I {
    type Output = Assert1sImpl<I, K, R>;

    fn assert_ones(self) -> Self::Output {
        Assert1sImpl(self)
    }
}
