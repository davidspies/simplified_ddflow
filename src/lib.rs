use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::{input, Collection};
use std::collections::hash_map::{Entry, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
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
    G: Fn(Context, T1) -> T2 + Send + Sync + 'static,
{
    timely::execute_from_args(iter, move |worker| {
        let mut register = InputRegister::new();
        let structures = worker.dataflow(|scope| setup(scope, &mut register));
        let mut context = Context {
            worker,
            register,
            current_step: 0,
        };
        context.commit();
        execute(context, structures)
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

impl<D: Clone + Ord + Debug + 'static, R: Semigroup> Registerable for InputSessionInner<D, R> {
    fn advance_to(&mut self, t: usize) {
        self.0.advance_to(t);
        self.0.flush();
    }
}

pub struct InputSession<D: Clone + Ord + Debug + 'static, R: Semigroup>(
    Arc<Mutex<InputSessionInner<D, R>>>,
);

impl<D: Clone + Ord + Debug + 'static, R: Semigroup> InputSession<D, R> {
    fn with_inner<T, F: FnOnce(&mut input::InputSession<usize, D, R>) -> T>(&mut self, f: F) -> T {
        f(&mut self.0.lock().unwrap().0)
    }
    pub fn to_collection<G: Scope<Timestamp = usize>>(
        &mut self,
        scope: &mut G,
    ) -> Collection<G, D, R> {
        self.with_inner(|i| i.to_collection(scope))
    }
    pub fn update(&mut self, element: D, change: R) {
        self.with_inner(|i| i.update(element, change))
    }
}

impl<D: Clone + Ord + Debug + 'static> InputSession<D, isize> {
    pub fn insert(&mut self, element: D) {
        self.with_inner(|i| i.insert(element))
    }
}

pub struct Context<'a> {
    worker: &'a mut Worker<Allocator>,
    register: InputRegister,
    current_step: usize,
}

impl<'a> Context<'a> {
    // Could potentially deadlock if another read was opened before calling commit and hasn't closed yet
    fn read_deadlocking<'c, 'b: 'c, D, R>(
        &mut self,
        r: &'b ReadRef<HashMap<D, R>>,
    ) -> RwLockReadGuard<'c, HashMap<D, R>> {
        while r.handle.less_than(&self.current_step) {
            self.worker.step();
        }
        r.data.read().unwrap()
    }
    /// Open a reference to a single collection output without creating a reader
    pub fn read<'c, 'b: 'c, D, R>(
        &'c mut self, // Although not necessary to compile, this lifetime annotation makes deadlock impossible since commit can't be called while the result is in scope
        r: &'b ReadRef<HashMap<D, R>>,
    ) -> RwLockReadGuard<'c, HashMap<D, R>> {
        self.read_deadlocking(r)
    }
    /// Create a reader for opening references to multiple collection outputs simultaneously
    pub fn get_reader<'c>(&'c mut self) -> Reader<'c, 'a> {
        // For opening references to multiple collections at the same time
        Reader(self)
    }
    pub fn commit(&mut self) {
        self.current_step += 1;
        let mut reg_refs: Vec<_> = (&self.register.0)
            .into_iter()
            .map(|r| r.lock().unwrap())
            .collect();
        for r in &mut reg_refs {
            r.advance_to(self.current_step);
        }
    }
}

pub struct Reader<'c, 'a>(&'c mut Context<'a>);

impl<'c, 'a> Reader<'c, 'a> {
    pub fn read<'b: 'c, D, R>(
        &mut self,
        r: &'b ReadRef<HashMap<D, R>>,
    ) -> RwLockReadGuard<'c, HashMap<D, R>> {
        self.0.read_deadlocking(r)
    }
}

pub trait CreateOutput<D, R> {
    fn create_output(&self) -> ReadRef<HashMap<D, R>>;
}

impl<G: Scope<Timestamp = usize>, D: Data + Eq + Hash, R: Monoid> CreateOutput<D, R>
    for Collection<G, D, R>
{
    fn create_output(&self) -> ReadRef<HashMap<D, R>> {
        let data = Arc::new(RwLock::new(HashMap::new()));
        let writer_ref = data.clone();
        self.inspect(move |(d, _, r)| {
            let mut data = writer_ref.write().unwrap();
            apply_update(&mut data, d.clone(), r.clone());
        });
        ReadRef {
            data,
            handle: self.probe(),
        }
    }
}

pub struct ReadRef<D> {
    data: Arc<RwLock<D>>,
    handle: Handle<usize>,
}

fn apply_update<D: Eq + Hash, R: Semigroup>(data: &mut HashMap<D, R>, k: D, v: R) {
    if v.is_zero() {
        return;
    }
    match data.entry(k) {
        Entry::Occupied(mut e) => {
            let val = e.get_mut();
            *val += &v;
            if val.is_zero() {
                e.remove_entry();
            }
        }
        Entry::Vacant(e) => {
            e.insert(v);
        }
    }
}
