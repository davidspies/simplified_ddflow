use differential_dataflow::difference::Semigroup;
use differential_dataflow::Collection;
use std::boxed::Box;
use std::collections::BTreeMap;
use std::mem;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use timely::communication::Allocator;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::scopes::Scope;
use timely::{worker::Worker, Data};

pub struct ContextOutput<'a> {
    worker: Mutex<&'a mut Worker<Allocator>>,
    current_step: usize,
}

pub fn new_context_output(worker: &mut Worker<Allocator>) -> ContextOutput {
    ContextOutput {
        worker: Mutex::new(worker),
        current_step: 0,
    }
}

pub fn increment_step(context: &mut ContextOutput) {
    context.current_step += 1;
}

pub fn get_current_step(context: &ContextOutput) -> usize {
    context.current_step
}

struct ReadRefInner<Y, D, R> {
    data: Y,
    pending_updates: BTreeMap<usize, Vec<(D, R)>>,
    f: Box<dyn FnMut(&mut Y, D, R)>,
}

impl<Y, D, R> ReadRefInner<Y, D, R> {
    fn new<F: FnMut(&mut Y, D, R) + 'static>(f: F) -> Self
    where
        Y: Default,
    {
        ReadRefInner {
            data: Default::default(),
            pending_updates: BTreeMap::new(),
            f: Box::new(f),
        }
    }
    fn advance_to(&mut self, cur_step: usize) {
        let mut popped = self.pending_updates.split_off(&cur_step);
        mem::swap(&mut self.pending_updates, &mut popped);
        for (_, v) in popped {
            for (d, r) in v {
                (*self.f)(&mut self.data, d, r)
            }
        }
    }
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
        context: &'c ContextOutput, // Although not necessary to compile, this lifetime annotation is important since it prevents deadlock by making sure the output ref gets dropped before the next commit call.
    ) -> ReadRefRef<'c, Y, D, R> {
        let cur_step = context.current_step;
        if self.handle.less_than(&cur_step) {
            // Avoid locking the worker if it's not necessary (yes this is double-checked locking, but I think it's fine here)
            let mut worker = context.worker.lock().unwrap();
            while self.handle.less_than(&cur_step) {
                worker.step();
            }
        }
        let data = &*self.data;
        let needs_updating = {
            // Must make sure this closes before calling `write` below
            let read_data = data.read().unwrap();
            read_data.pending_updates.range(..cur_step).next().is_some()
        };
        if needs_updating {
            // More double-checked locking
            data.write().unwrap().advance_to(cur_step);
        }
        ReadRefRef(data.read().unwrap())
    }
}

pub trait CreateUpdater<D, R> {
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, D, R) + 'static>(
        &self,
        f: F,
    ) -> ReadRef<Y, D, R>;
}

impl<G: Scope<Timestamp = usize>, D: Data, R: Semigroup> CreateUpdater<D, R>
    for Collection<G, D, R>
{
    fn create_updater<Y: Default + 'static, F: FnMut(&mut Y, D, R) + 'static>(
        &self,
        f: F,
    ) -> ReadRef<Y, D, R> {
        let data = Arc::new(RwLock::new(ReadRefInner::new(f)));
        let writer_ref = Arc::downgrade(&data);
        self.inspect(move |&(ref d, t, ref r)| {
            if let Some(data) = writer_ref.upgrade() {
                let pending_updates = &mut data.write().unwrap().pending_updates;
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
