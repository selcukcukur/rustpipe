pub mod error;
mod utility;

use std::any::type_name;
#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;
use crate::error::{PipelineError, PipelineResult, StepFailure};

pub trait Pipe<T, E> {
    fn handle(&self, passable: T) -> Result<T, E>;
}

/// A configurable pipeline for sequential data processing.
///
/// **Generics**
/// - `TPassable` - The type of the value that flows through the pipeline.
/// - `TError` - The error type returned by pipes when processing fails.
pub struct Pipeline<TPassable, TError> {
    /// The optional input value passed into the pipeline.
    /// This is provided via [`Pipeline::send`] and becomes
    /// the starting point for all subsequent pipes.
    passable: Option<TPassable>,

    /// A vector of boxed pipes (`Box<dyn Pipe<TPassable, TError>>`)
    /// that will be executed sequentially. Each pipe transforms
    /// the input or returns an error.
    pipes: Vec<Box<dyn Pipe<TPassable, TError>>>,

    /// A collection of observer closures (`Fn(&TPassable)`) that run
    /// after each successful pipe execution. These taps allow
    /// side effects such as logging, metrics, or debugging
    /// without modifying the pipeline value itself.
    taps: Vec<Box<dyn Fn(&TPassable)>>,
}

impl<TPassable, TError: std::fmt::Debug> Pipeline<TPassable, TError> where PipelineError: From<TError> {
    /// Creates a new, empty pipeline instance.
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),
            taps: Vec::new(),
        }
    }

    /// Provides the initial passable value to the pipeline.
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Intercepts errors and allows recovery via a closure.
    pub fn rescue<F>(self, f: F) -> PipelineResult<TPassable>
    where
        F: FnOnce(PipelineError) -> TPassable,
    {
        let mut passable = self.passable.ok_or(PipelineError::InputMissing)?;
        for step in &self.pipes {
            match step.handle(passable) {
                Ok(val) => passable = val,
                Err(err) => {
                    return Err(utility::step_failure_from::<TError, TPassable>(err).into())
                }
            }
        }
        Ok(passable)
    }

    /// Observes the current pipeline passable without modifying it.
    pub fn tap<F>(mut self, f: F) -> Self
    where
        F: Fn(&TPassable) + 'static,
    {
        self.taps.push(Box::new(f));
        self
    }

    /// Adds a sequence of pipes to the pipeline.
    pub fn through(mut self, pipes: Vec<Box<dyn Pipe<TPassable, TError>>>) -> Self {
        for step in pipes {
            self.pipes.push(step);
        }
        self
    }

    /// Executes the pipeline and applies a final transformation closure to the result.
    pub fn then<F, R>(self, f: F) -> PipelineResult<R>
    where
        F: FnOnce(TPassable) -> R,
    {
        let mut passable = self.passable.ok_or(PipelineError::InputMissing)?;
        for step in &self.pipes {
            match step.handle(passable) {
                Ok(val) => passable = val,
                Err(err) => {
                    return Err(utility::step_failure_from::<TError, TPassable>(err).into())
                }
            }
        }
        Ok(f(passable))
    }

    /// Finalizes the pipeline and returns the processed output.
    pub fn then_return(self) -> PipelineResult<TPassable> {
        let mut passable = utility::require_passable(self.passable)?;
        for step in &self.pipes {
            match step.handle(passable) {
                Ok(val) => {
                    passable = val;
                    utility::run_taps(&self.taps, &passable);
                }
                Err(err) => {
                    return Err(utility::step_failure_from::<TError, TPassable>(err).into());
                }
            }
        }
        Ok(passable)
    }

    /// Adds a step that runs only if condition is true.
    pub fn when(mut self, condition: bool, step: Box<dyn Pipe<TPassable, TError>>) -> Self {
        if condition {
            self.pipes.push(step);
        }
        self
    }

    /// Adds a step that runs only if condition is false.
    pub fn unless(mut self, condition: bool, step: Box<dyn Pipe<TPassable, TError>>) -> Self {
        if !condition {
            self.pipes.push(step);
        }
        self
    }
}

#[cfg(feature = "async")]
pub trait AsyncPipe<T, E> {
    fn handle<'a>(&'a self, passable: T) -> Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;
}

#[cfg(feature = "async")]
pub struct AsyncPipeline<T, E> {
    pipes: Vec<Box<dyn AsyncPipe<T, E>>>,
}

#[cfg(feature = "async")]
impl<T, E> AsyncPipeline<T, E> {
    pub fn new() -> Self {
        Self { pipes: Vec::new() }
    }

    pub fn add<P: AsyncPipe<T, E> + 'static>(mut self, step: P) -> Self {
        self.pipes.push(Box::new(step));
        self
    }

    pub async fn execute(&self, mut passable: T) -> Result<T, E> {
        for step in &self.pipes {
            passable = step.handle(passable).await?;
        }
        Ok(passable)
    }
}