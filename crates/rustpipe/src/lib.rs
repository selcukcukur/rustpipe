mod error;

#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;
use crate::error::{PipelineError, PipelineResult};

/// Defines a single processing step in a pipeline.
///
/// # Overview
/// The `Pipe` trait represents a unit of work that can transform an input value
/// or return an error. Each step in a [`Pipeline`] must implement this trait.
/// Steps are executed sequentially in the order they are added.
///
/// # Method
/// - `handle(&self, input: T) -> Result<T, E>`
///   Processes the given input and either returns a transformed value (`Ok(T)`)
///   or an error (`Err(E)`).
///
/// # Type Parameters
/// - `T`: The type of the input and output value.
/// - `E`: The error type returned if processing fails.
///
/// # Usage
/// Implement this trait for any struct that should act as a pipeline step.
/// Each step can perform transformations, validations, or other operations.
///
/// # Example
/// ```
/// use rustpipe::{Pipeline, Pipe};
///
/// struct TrimStep;
/// impl Pipe<String, String> for TrimStep {
///     fn handle(&self, input: String) -> Result<String, String> {
///         Ok(input.trim().to_string())
///     }
/// }
///
/// struct UpperStep;
/// impl Pipe<String, String> for UpperStep {
///     fn handle(&self, input: String) -> Result<String, String> {
///         Ok(input.to_uppercase())
///     }
/// }
///
/// fn main() {
///     let result = Pipeline::new()
///         .send("   hello rustpipe   ".to_string())
///         .through(vec![Box::new(TrimStep), Box::new(UpperStep)])
///         .then_return();
///
///     assert_eq!(result.unwrap(), "HELLO RUSTPIPE");
/// }
/// ```
pub trait Pipe<T, E> {
    fn handle(&self, input: T) -> Result<T, E>;
}

/// A configurable pipeline for sequential data processing.
///
/// # Overview
/// The `Pipeline` struct provides a mechanism to send an input value through
/// a series of steps. Each step must implement the [`Pipe`] trait and can
/// transform the input or return an error. The pipeline supports chaining
/// methods for ergonomic usage.
///
/// # Fields
/// - `input`: The optional input value of type `T` provided via [`Pipeline::send`].
/// - `steps`: A vector of boxed steps (`Box<dyn Pipe<T, E>>`) that will be executed in order.
/// - `method`: The name of the method to call on each step (default is `"handle"`).
///
/// # Usage
/// 1. Create a new pipeline with [`Pipeline::new`].
/// 2. Provide input using [`Pipeline::send`].
/// 3. Add steps with [`Pipeline::through`].
/// 4. Optionally configure dispatch with [`Pipeline::via`] or observe state with [`Pipeline::tap`].
/// 5. Finalize execution with [`Pipeline::then_return`] or [`Pipeline::then`].
///
/// # Example
/// ```
/// use rustpipe::{Pipeline, Pipe};
///
/// struct TrimStep;
/// impl Pipe<String, String> for TrimStep {
///     fn handle(&self, input: String) -> Result<String, String> {
///         Ok(input.trim().to_string())
///     }
/// }
///
/// struct UpperStep;
/// impl Pipe<String, String> for UpperStep {
///     fn handle(&self, input: String) -> Result<String, String> {
///         Ok(input.to_uppercase())
///     }
/// }
///
/// fn main() {
///     let result = Pipeline::<String, String>::new()
///         .send("   hello rustpipe   ".to_string())
///         .through(vec![Box::new(TrimStep), Box::new(UpperStep)])
///         .then_return();
///
///     assert_eq!(result.unwrap(), "HELLO RUSTPIPE");
/// }
/// ```
pub struct Pipeline<T, E> {
    input: Option<T>,
    steps: Vec<Box<dyn Pipe<T, E>>>,
    taps: Vec<Box<dyn Fn(&T)>>
}

impl<T, E: std::fmt::Debug> Pipeline<T, E> where PipelineError: From<E> {
    /// Creates a new, empty pipeline instance.
    pub fn new() -> Self {
        Self {
            input: None,
            steps: Vec::new(),
            taps: Vec::new(),
        }
    }

    /// Provides the initial input value to the pipeline.
    pub fn send(mut self, input: T) -> Self {
        self.input = Some(input);
        self
    }

    /// Intercepts errors and allows recovery via a closure.
    pub fn rescue<F>(self, f: F) -> PipelineResult<T>
    where
        F: FnOnce(PipelineError) -> T,
    {
        let mut input = self.input.ok_or(PipelineError::InputMissing)?;
        for step in &self.steps {
            match step.handle(input) {
                Ok(val) => input = val,
                Err(err) => return Ok(f(err.into()))
            }
        }
        Ok(input)
    }

    /// Observes the current pipeline input without modifying it.
    pub fn tap<F>(mut self, f: F) -> Self
    where
        F: Fn(&T) + 'static,
    {
        self.taps.push(Box::new(f));
        self
    }

    /// Adds a sequence of steps to the pipeline.
    pub fn through(mut self, steps: Vec<Box<dyn Pipe<T, E>>>) -> Self {
        for step in steps {
            self.steps.push(step);
        }
        self
    }

    /// Executes the pipeline and applies a final transformation closure to the result.
    pub fn then<F, R>(self, f: F) -> PipelineResult<R>
    where
        F: FnOnce(T) -> R,
    {
        let mut input = self.input.ok_or(PipelineError::InputMissing)?;
        for step in &self.steps {
            input = step.handle(input)?;
        }
        Ok(f(input))
    }

    /// Finalizes the pipeline and returns the processed output.
    pub fn then_return(self) -> PipelineResult<T> {
        let mut input = self.input.ok_or(PipelineError::InputMissing)?;
        for step in &self.steps {
            input = step.handle(input)?;
            for tap in &self.taps {
                tap(&input);
            }
        }
        Ok(input)
    }

    /// Adds a step that runs only if condition is true.
    pub fn when<F>(mut self, condition: bool, step: Box<dyn Pipe<T, E>>) -> Self {
        if condition {
            self.steps.push(step);
        }
        self
    }

    /// Adds a step that runs only if condition is false.
    pub fn unless<F>(mut self, condition: bool, step: Box<dyn Pipe<T, E>>) -> Self {
        if !condition {
            self.steps.push(step);
        }
        self
    }
}

#[cfg(feature = "async")]
pub trait AsyncPipe<T, E> {
    fn handle<'a>(&'a self, input: T) -> Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;
}

#[cfg(feature = "async")]
pub struct AsyncPipeline<T, E> {
    steps: Vec<Box<dyn AsyncPipe<T, E>>>,
}

#[cfg(feature = "async")]
impl<T, E> AsyncPipeline<T, E> {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn add<P: AsyncPipe<T, E> + 'static>(mut self, step: P) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    pub async fn execute(&self, mut input: T) -> Result<T, E> {
        for step in &self.steps {
            input = step.handle(input).await?;
        }
        Ok(input)
    }
}