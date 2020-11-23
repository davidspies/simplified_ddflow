use std::sync::{Arc, Mutex};
use timely::communication::{Allocator, WorkerGuards};
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

mod input;
mod output;

use input::*;
pub use input::{ContextInput, InputRegister, InputSession};
use output::*;
pub use output::{ContextOutput, CreateUpdater, ReadRef, ReadRefRef};

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
        let (receiver, structures) = {
            let (mut register, receiver) = new_input_register();
            let structures = worker.dataflow(|scope| setup(scope, &mut register));
            (receiver, structures)
        };
        let mut context = Context {
            input: new_context_input(),
            output: new_context_output(worker),
            registered: receiver.iter().collect(),
        };
        execute(&mut context, structures)
    })
}

pub struct Context<'a> {
    input: ContextInput,
    output: ContextOutput<'a>,
    registered: Vec<Arc<Mutex<dyn Registerable>>>,
}

impl<'a> Context<'a> {
    pub fn get_input<'b>(&'b self) -> &'b ContextInput {
        &self.input
    }
    pub fn get_output<'b>(&'b self) -> &'b ContextOutput<'a> {
        &self.output
    }
    pub fn commit(&mut self) {
        increment_step(&mut self.output);
        for r in self.registered.iter() {
            r.lock().unwrap().advance_to(get_current_step(&self.output));
        }
    }
}
