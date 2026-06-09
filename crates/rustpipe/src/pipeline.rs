pub trait Pipe<T, E> {
    fn handle(&self, input: T) -> Result<T, E>;
}

pub struct Pipeline<T, E> {
    input: Option<T>,
    steps: Vec<Box<dyn Pipe<T, E>>>,
}

impl<T, E> Pipeline<T, E> {
    pub fn new() -> Self {
        Self { input: None, steps: Vec::new() }
    }

    pub fn send(mut self, input: T) -> Self {
        self.input = Some(input);
        self
    }

    pub fn through<P: Pipe<T, E> + 'static>(mut self, steps: Vec<P>) -> Self {
        for step in steps {
            self.steps.push(Box::new(step));
        }
        self
    }

    pub fn then_return(self) -> Result<T, E> {
        let mut input = self.input.expect("Pipeline input not set");
        for step in &self.steps {
            input = step.handle(input)?;
        }
        Ok(input)
    }
}
