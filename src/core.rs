use differential_dataflow::difference::Semigroup;
use differential_dataflow::{input, Collection};
use std::fmt::Debug;
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
    pub fn get_input<'b>(&'b mut self) -> ContextInput<'b> {
        ContextInput(PhantomData)
    }
    pub fn get_output<'b>(&'b self) -> ContextOutput<'b, 'a> {
        ContextOutput(self)
    }
    pub fn commit(&mut self) {
        self.current_step += 1;
        for r in &mut self.register.0 {
            r.lock().unwrap().advance_to(self.current_step);
        }
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

pub trait CreateUpdater<D, R> {
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, &D, &R) + 'static>(
        &self,
        f: F,
    ) -> ReadRef<Y>;
}

impl<G: Scope<Timestamp = usize>, D: Data, R: Semigroup> CreateUpdater<D, R>
    for Collection<G, D, R>
{
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, &D, &R) + 'static>(
        &self,
        mut f: F,
    ) -> ReadRef<Y> {
        let data = Arc::new(RwLock::new(Default::default()));
        let writer_ref = Arc::downgrade(&data);
        self.inspect(move |(d, _, r)| {
            if let Some(data) = writer_ref.upgrade() {
                f(&mut data.write().unwrap(), d, r)
            }
        });
        ReadRef {
            data,
            handle: self.probe(),
        }
    }
}
