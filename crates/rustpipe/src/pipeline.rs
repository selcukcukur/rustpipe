pub trait Pipe<T, E> {
    fn handle(&self, input: T) -> Result<T, E>;
}

pub struct Pipeline<T, E> {
    input: Option<T>,
    steps: Vec<Box<dyn Pipe<T, E>>>,
    method: String,
}

impl<T, E> Pipeline<T, E> {
    pub fn new() -> Self {
        Self { input: None, steps: Vec::new(), method: "handle".to_string() }
    }

    pub fn send(mut self, input: T) -> Self {
        self.input = Some(input);
        self
    }

    pub fn via(mut self, method: &str) -> Self {
        self.method = method.to_string();
        self
    }

    pub fn tap<F>(self, f: F) -> Self
    where
        F: Fn(&T) + 'static,
    {
        if let Some(ref input) = self.input {
            f(input);
        }
        self
    }

    pub fn through(mut self, steps: Vec<Box<dyn Pipe<T, E>>>) -> Self {
        for step in steps {
            self.steps.push(step);
        }
        self
    }

    pub fn then<F, R>(self, f: F) -> Result<R, E>
    where
        F: FnOnce(T) -> R,
    {
        let mut input = self.input.expect("Pipeline input not set");
        for step in &self.steps {
            input = step.handle(input)?;
        }
        Ok(f(input))
    }

    pub fn then_return(self) -> Result<T, E> {
        let mut input = self.input.expect("Pipeline input not set");
        for step in &self.steps {
            input = step.handle(input)?;
        }
        Ok(input)
    }
}
