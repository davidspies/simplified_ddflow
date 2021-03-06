use differential_dataflow::difference::Semigroup;
use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use std::hash::Hash;
use std::iter::FromIterator;
use std::mem;

pub trait SemigroupWrapper {
    type Wrapped: Default;

    fn wrap(x: Self::Wrapped) -> Self;
    fn unwrap(self) -> Self::Wrapped;
    fn is_zero(&self) -> bool;
    fn incorporate(&mut self, right: Self);
}

pub struct SG<T>(pub T);

impl<T: Default + Semigroup> SemigroupWrapper for SG<T> {
    type Wrapped = T;

    fn wrap(x: Self::Wrapped) -> Self {
        SG(x)
    }
    fn unwrap(self) -> Self::Wrapped {
        self.0
    }
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
    fn incorporate(&mut self, other: Self) {
        (*self).0 += &other.0;
    }
}

pub struct SGH<K, V: SemigroupWrapper>(pub HashMap<K, <V as SemigroupWrapper>::Wrapped>);

impl<K: Eq + Hash, V: SemigroupWrapper> SemigroupWrapper for SGH<K, V> {
    type Wrapped = HashMap<K, <V as SemigroupWrapper>::Wrapped>;

    fn wrap(x: Self::Wrapped) -> Self {
        SGH(x)
    }
    fn unwrap(self) -> Self::Wrapped {
        self.0
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
    fn incorporate(&mut self, other: Self) {
        for (k, v) in other.0.into_iter() {
            apply_hash_update(&mut self.0, k, V::wrap(v))
        }
    }
}

pub fn apply_hash_update<D: Eq + Hash, R: SemigroupWrapper>(
    data: &mut HashMap<D, <R as SemigroupWrapper>::Wrapped>,
    k: D,
    v: R,
) {
    if v.is_zero() {
        return;
    }
    match data.entry(k) {
        hash_map::Entry::Occupied(mut e) => {
            let val = e.get_mut();
            let mut valw = R::wrap(mem::take(val));
            valw.incorporate(v);
            if valw.is_zero() {
                e.remove_entry();
            } else {
                *val = valw.unwrap();
            }
        }
        hash_map::Entry::Vacant(e) => {
            e.insert(v.unwrap());
        }
    }
}

pub fn apply_btree_update<D: Ord, R: SemigroupWrapper>(
    data: &mut BTreeMap<D, <R as SemigroupWrapper>::Wrapped>,
    k: D,
    v: R,
) {
    if v.is_zero() {
        return;
    }
    match data.entry(k) {
        btree_map::Entry::Occupied(mut e) => {
            let val = e.get_mut();
            let mut valw = R::wrap(mem::take(val));
            valw.incorporate(v);
            if valw.is_zero() {
                e.remove_entry();
            } else {
                *val = valw.unwrap();
            }
        }
        btree_map::Entry::Vacant(e) => {
            e.insert(v.unwrap());
        }
    }
}

fn singleton_hash_map<V: Eq + Hash, R: SemigroupWrapper>(k: V, vw: R) -> SGH<V, R> {
    SGH::<V, R>(HashMap::from_iter(vec![(k, vw.unwrap())]))
}

pub fn apply_map_update<K: Eq + Hash, V: Eq + Hash, R: SemigroupWrapper>(
    data: &mut HashMap<K, HashMap<V, <R as SemigroupWrapper>::Wrapped>>,
    k: (K, V),
    vw: R,
) {
    apply_hash_update(data, k.0, singleton_hash_map(k.1, vw))
}

pub fn apply_btree_map_update<K: Ord, V: Eq + Hash, R: SemigroupWrapper>(
    data: &mut BTreeMap<K, HashMap<V, <R as SemigroupWrapper>::Wrapped>>,
    k: (K, V),
    vw: R,
) {
    apply_btree_update(data, k.0, singleton_hash_map(k.1, vw))
}
