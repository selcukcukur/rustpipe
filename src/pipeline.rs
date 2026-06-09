pub trait Pipe<T> {
    fn handle(&self, input: T) -> T;
}

pub struct Pipeline<T> {
    steps: Vec<Box<dyn Pipe<T>>>,
}

impl<T> Pipeline<T> {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn add<P: Pipe<T> + 'static>(mut self, step: P) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    pub fn execute(&self, mut input: T) -> T {
        for step in &self.steps {
            input = step.handle(input);
        }
        input
    }
}
