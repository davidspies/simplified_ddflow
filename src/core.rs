use differential_dataflow::difference::Semigroup;
use differential_dataflow::{input, Collection};
use std::boxed::Box;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
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

struct ReadRefInner<Y, D, R> {
    data: Y,
    pending_updates: BTreeMap<usize, Vec<(D, R)>>,
    f: Box<dyn FnMut(&mut Y, &D, &R)>,
}

pub struct ReadRefRef<'a, Y, D, R>(RwLockReadGuard<'a, ReadRefInner<Y, D, R>>);

impl<Y, D, R> Deref for ReadRefRef<'_, Y, D, R> {
    type Target = Y;
    fn deref(&self) -> &Self::Target {
        &self.0.data
    }
}

pub struct ReadRef<Y, D, R> {
    data: Arc<RwLock<ReadRefInner<Y, D, R>>>,
    handle: Handle<usize>,
}

impl<Y, D, R> ReadRef<Y, D, R> {
    pub fn read<'c>(
        &'c self,
        &ContextOutput(context): &'c ContextOutput, // Although not necessary to compile, this lifetime annotation is important since it prevents deadlock by making sure the output ref gets dropped before the next commit call.
    ) -> ReadRefRef<'c, Y, D, R> {
        if self.handle.less_than(&context.current_step) {
            // Avoid locking the worker if it's not necessary (yes this is double-checked locking, but I think it's fine here)
            let mut worker = context.worker.lock().unwrap();
            while self.handle.less_than(&context.current_step) {
                worker.step();
            }
        }
        if self
            .data
            .read()
            .unwrap()
            .pending_updates
            .range(..context.current_step)
            .next()
            .is_some()
        {
            let ReadRefInner {
                ref mut pending_updates,
                ref mut f,
                ref mut data,
            } = *self.data.write().unwrap();
            let mut popped = pending_updates.split_off(&context.current_step);
            mem::swap(pending_updates, &mut popped);
            for (_, v) in popped {
                for (d, r) in v {
                    (*f)(data, &d, &r)
                }
            }
        }
        ReadRefRef(self.data.read().unwrap())
    }
}

pub trait CreateUpdater<D, R> {
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, &D, &R) + 'static>(
        &self,
        f: F,
    ) -> ReadRef<Y, D, R>;
}

impl<G: Scope<Timestamp = usize>, D: Data, R: Semigroup> CreateUpdater<D, R>
    for Collection<G, D, R>
{
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, &D, &R) + 'static>(
        &self,
        f: F,
    ) -> ReadRef<Y, D, R> {
        let data = Arc::new(RwLock::new(ReadRefInner {
            data: Default::default(),
            pending_updates: BTreeMap::new(),
            f: Box::new(f),
        }));
        let writer_ref = Arc::downgrade(&data);
        self.inspect(move |&(ref d, t, ref r)| {
            if let Some(data) = writer_ref.upgrade() {
                let &mut ReadRefInner {
                    ref mut pending_updates,
                    ..
                } = &mut *data.write().unwrap();
                pending_updates
                    .entry(t)
                    .or_default()
                    .push((d.clone(), r.clone()));
            }
        });
        ReadRef {
            data,
            handle: self.probe(),
        }
    }
}
