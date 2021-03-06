use differential_dataflow::difference::Semigroup;
use differential_dataflow::{input, Collection};
use std::fmt::Debug;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use timely::dataflow::scopes::Scope;

struct InputSessionInner<D: Clone + Ord + Debug + 'static, R: Semigroup>(
    input::InputSession<usize, D, R>,
);

pub struct InputRegister(Sender<Arc<Mutex<dyn Registerable>>>);

pub fn new_input_register() -> (InputRegister, Receiver<Arc<Mutex<dyn Registerable>>>) {
    let (sender, receiver) = mpsc::channel();
    (InputRegister(sender), receiver)
}

impl InputRegister {
    pub fn create_input<D: Clone + Ord + Debug + 'static, R: Semigroup>(
        &self,
    ) -> InputSession<D, R> {
        let sess = InputSessionInner(input::InputSession::new());
        let res = Arc::from(Mutex::new(sess));
        self.0.send(res.clone()).unwrap_or_default();
        InputSession(res)
    }
}

pub trait Registerable {
    fn advance_to(&mut self, t: usize);
}

impl<D: Clone + Ord + Debug, R: Semigroup> Registerable for InputSessionInner<D, R> {
    fn advance_to(&mut self, t: usize) {
        self.0.advance_to(t);
        self.0.flush();
    }
}

pub struct InputSession<D: Clone + Ord + Debug + 'static, R: Semigroup = isize>(
    Arc<Mutex<InputSessionInner<D, R>>>,
);

impl<D: Clone + Ord + Debug, R: Semigroup> InputSession<D, R> {
    fn with_inner<T, F: FnOnce(&mut input::InputSession<usize, D, R>) -> T>(&self, f: F) -> T {
        f(&mut self.0.lock().unwrap().0)
    }
    pub fn to_collection<G: Scope<Timestamp = usize>>(&self, scope: &mut G) -> Collection<G, D, R> {
        self.with_inner(|i| i.to_collection(scope))
    }
    pub fn update(&self, _: &ContextInput, element: D, change: R) {
        self.with_inner(|i| i.update(element, change))
    }
}

impl<D: Clone + Ord + Debug> InputSession<D, isize> {
    pub fn insert(&self, _: &ContextInput, element: D) {
        self.with_inner(|i| i.insert(element))
    }
}

pub fn new_context_input() -> ContextInput {
    ContextInput(())
}

pub struct ContextInput(());
