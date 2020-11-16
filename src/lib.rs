use differential_dataflow::difference::Semigroup;
use differential_dataflow::{input, Collection};
use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use timely::communication::{Allocator, WorkerGuards};
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::scopes::{Child, Scope};
use timely::{worker::Worker, Data};

pub fn execute_from_args<I, T1, T2: Send + Sync, F, G>(
    iter: I,
    setup: F,
    execute: G,
) -> Result<WorkerGuards<T2>, String>
where
    I: Iterator<Item = String>,
    F: Fn(&mut Child<Worker<Allocator>, usize>, &mut InputRegister) -> T1 + Send + Sync + 'static,
    G: Fn(&mut Context, T1) -> T2 + Send + Sync + 'static,
{
    timely::execute_from_args(iter, move |worker| {
        let mut register = InputRegister::new();
        let structures = worker.dataflow(|scope| setup(scope, &mut register));
        let mut context = Context {
            worker: Mutex::new(worker),
            current_step: 0,
            register,
        };
        execute(&mut context, structures)
    })
}

struct InputSessionInner<D: Clone + Ord + Debug + 'static, R: Semigroup>(
    input::InputSession<usize, D, R>,
);

pub struct InputRegister(Vec<Arc<Mutex<dyn Registerable>>>);

impl InputRegister {
    fn new() -> Self {
        InputRegister(Vec::new())
    }
    pub fn create_input<D: Clone + Ord + Debug + 'static, R: Semigroup>(
        &mut self,
    ) -> InputSession<D, R> {
        let sess = InputSessionInner(input::InputSession::new());
        let res = Arc::from(Mutex::new(sess));
        self.0.push(res.clone());
        InputSession(res)
    }
}

trait Registerable {
    fn advance_to(&mut self, t: usize);
}

impl<D: Clone + Ord + Debug, R: Semigroup> Registerable for InputSessionInner<D, R> {
    fn advance_to(&mut self, t: usize) {
        self.0.advance_to(t);
        self.0.flush();
    }
}

pub struct InputSession<D: Clone + Ord + Debug + 'static, R: Semigroup>(
    Arc<Mutex<InputSessionInner<D, R>>>,
);

impl<D: Clone + Ord + Debug, R: Semigroup> InputSession<D, R> {
    fn with_inner<T, F: FnOnce(&mut input::InputSession<usize, D, R>) -> T>(&self, f: F) -> T {
        f(&mut self.0.lock().unwrap().0)
    }
    pub fn to_collection<G: Scope<Timestamp = usize>>(&self, scope: &mut G) -> Collection<G, D, R> {
        self.with_inner(|i| i.to_collection(scope))
    }
    pub fn update(&self, _: &mut ContextInput, element: D, change: R) {
        self.with_inner(|i| i.update(element, change))
    }
}

impl<D: Clone + Ord + Debug> InputSession<D, isize> {
    pub fn insert(&self, _: &mut ContextInput, element: D) {
        self.with_inner(|i| i.insert(element))
    }
}

pub struct Context<'a> {
    worker: Mutex<&'a mut Worker<Allocator>>,
    current_step: usize,
    register: InputRegister,
}

// We're pretending that the input data to be commited is stored in this ContextInput. Hence the dummy lifetime.
pub struct ContextInput<'b>(PhantomData<&'b ()>);
pub struct ContextOutput<'b, 'a>(&'b Context<'a>);

impl<'a> Context<'a> {
    pub fn get_io<'b>(&'b mut self) -> (ContextInput<'b>, ContextOutput<'b, 'a>) {
        (ContextInput(PhantomData), ContextOutput(self))
    }
    pub fn commit(&mut self) {
        self.current_step += 1;
        for r in &mut self.register.0 {
            r.lock().unwrap().advance_to(self.current_step);
        }
    }
}

pub trait CreateHashedOutput<D, R> {
    fn create_hashed_output(&self) -> ReadRef<HashMap<D, R>>;
}
pub trait CreateOrderedOutput<D, R> {
    fn create_ordered_output(&self) -> ReadRef<BTreeMap<D, R>>;
}
pub trait CreateCountOutput<D, R> {
    fn create_count_output(&self) -> ReadRef<R>;
}
pub trait CreateMapOutput<K, V, R> {
    fn create_map_output(&self) -> ReadRef<HashMap<K, HashMap<V, R>>>;
}

fn create_updater<
    G: Scope<Timestamp = usize>,
    D: Data,
    R: Semigroup,
    Y: Default + 'static,
    F: FnMut(&mut Y, &D, &R) + 'static,
>(
    rel: &Collection<G, D, R>,
    mut f: F,
) -> ReadRef<Y> {
    let data = Arc::new(RwLock::new(Default::default()));
    let writer_ref = Arc::downgrade(&data);
    rel.inspect(move |(d, _, r)| {
        if let Some(data) = writer_ref.upgrade() {
            f(&mut data.write().unwrap(), d, r)
        }
    });
    ReadRef {
        data,
        handle: rel.probe(),
    }
}

impl<G: Scope<Timestamp = usize>, D: Data + Eq + Hash, R: Semigroup> CreateHashedOutput<D, R>
    for Collection<G, D, R>
{
    fn create_hashed_output(&self) -> ReadRef<HashMap<D, R>> {
        create_updater(self, |data, d, r| {
            apply_hash_update(data, d.clone(), SG(r.clone()))
        })
    }
}
impl<G: Scope<Timestamp = usize>, D: Data + Ord, R: Semigroup> CreateOrderedOutput<D, R>
    for Collection<G, D, R>
{
    fn create_ordered_output(&self) -> ReadRef<BTreeMap<D, R>> {
        create_updater(self, |data, d, r| {
            apply_btree_update(data, d.clone(), r.clone())
        })
    }
}
impl<G: Scope<Timestamp = usize>, D: Data, R: Default + Semigroup> CreateCountOutput<D, R>
    for Collection<G, D, R>
{
    fn create_count_output(&self) -> ReadRef<R> {
        create_updater(self, |data, _, r| *data += r)
    }
}

impl<
        G: Scope<Timestamp = usize>,
        K: Data + Eq + Hash,
        V: Data + Eq + Hash,
        R: Default + Semigroup,
    > CreateMapOutput<K, V, R> for Collection<G, (K, V), R>
{
    fn create_map_output(&self) -> ReadRef<HashMap<K, HashMap<V, R>>> {
        create_updater(self, |data, d, r| {
            apply_map_update(data, d.clone(), SG(r.clone()))
        })
    }
}

pub struct ReadRef<D> {
    data: Arc<RwLock<D>>,
    handle: Handle<usize>,
}

impl<T> ReadRef<T> {
    pub fn read<'c>(
        &'c self,
        ContextOutput(context): &'c ContextOutput, // Although not necessary to compile, this lifetime annotation is important since it prevents deadlock by making sure the output ref gets dropped before the next commit call.
    ) -> RwLockReadGuard<'c, T> {
        if self.handle.less_than(&context.current_step) {
            // Avoid locking the worker if it's not necessary (yes this is double-checked locking, but I think it's fine here)
            let mut worker = context.worker.lock().unwrap();
            while self.handle.less_than(&context.current_step) {
                worker.step();
            }
        }
        self.data.read().unwrap()
    }
}

impl<D: Clone + Ord + Debug, R: Semigroup> ReadRef<HashMap<D, R>> {
    pub fn feedback(self: &Self, context: &mut Context, input: &InputSession<D, R>) {
        let (mut context_input, context_output) = context.get_io();
        for (k, v) in self.read(&context_output).iter() {
            input.update(&mut context_input, k.clone(), v.clone());
        }
    }
}

trait SemigroupWrapper {
    type Output;

    fn wrap(x: Self::Output) -> Self;
    fn unwrap(self) -> Self::Output;
    fn is_zero(x: &Self::Output) -> bool;
    fn incorporate(left: &mut Self::Output, right: Self::Output);
}

struct SG<T>(T);

impl<T: Semigroup> SemigroupWrapper for SG<T> {
    type Output = T;

    fn wrap(x: Self::Output) -> Self {
        SG(x)
    }
    fn unwrap(self) -> Self::Output {
        self.0
    }
    fn is_zero(x: &Self::Output) -> bool {
        x.is_zero()
    }
    fn incorporate(left: &mut Self::Output, right: Self::Output) {
        *left += &right;
    }
}

struct SGH<K, V: SemigroupWrapper>(HashMap<K, <V as SemigroupWrapper>::Output>);

impl<K: Eq + Hash, V: SemigroupWrapper> SemigroupWrapper for SGH<K, V> {
    type Output = HashMap<K, <V as SemigroupWrapper>::Output>;

    fn wrap(x: Self::Output) -> Self {
        SGH(x)
    }
    fn unwrap(self) -> Self::Output {
        self.0
    }
    fn is_zero(x: &Self::Output) -> bool {
        x.is_empty()
    }
    fn incorporate(left: &mut Self::Output, right: Self::Output) {
        for (k, v) in right.into_iter() {
            apply_hash_update(left, k, V::wrap(v))
        }
    }
}

fn apply_hash_update<D: Eq + Hash, R: SemigroupWrapper>(
    data: &mut HashMap<D, <R as SemigroupWrapper>::Output>,
    k: D,
    vw: R,
) {
    let v = R::unwrap(vw);
    if R::is_zero(&v) {
        return;
    }
    match data.entry(k) {
        hash_map::Entry::Occupied(mut e) => {
            let val = e.get_mut();
            R::incorporate(val, v);
            if R::is_zero(val) {
                e.remove_entry();
            }
        }
        hash_map::Entry::Vacant(e) => {
            e.insert(v);
        }
    }
}

fn apply_btree_update<D: Ord, R: Semigroup>(data: &mut BTreeMap<D, R>, k: D, v: R) {
    if v.is_zero() {
        return;
    }
    match data.entry(k) {
        btree_map::Entry::Occupied(mut e) => {
            let val = e.get_mut();
            *val += &v;
            if val.is_zero() {
                e.remove_entry();
            }
        }
        btree_map::Entry::Vacant(e) => {
            e.insert(v);
        }
    }
}

fn apply_map_update<K: Eq + Hash, V: Eq + Hash, R: SemigroupWrapper>(
    data: &mut HashMap<K, HashMap<V, <R as SemigroupWrapper>::Output>>,
    k: (K, V),
    vw: R,
) {
    apply_hash_update(
        data,
        k.0,
        SGH::<V, R>(HashMap::from_iter(vec![(k.1, vw.unwrap())])),
    )
}
