pub mod errors;
pub mod utility;
pub mod types;

#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;
use std::sync::Arc;
pub use crate::errors::*;
pub use crate::types::*;

/// A single processing unit within a [`Pipeline`].
///
/// **Generics**
/// - `TPassable` - The type of the value that flows through the pipeline.
/// - `TError` - The error type returned when a pipe fails.
///
/// **Returns**
/// - `Ok(TPassable)` with the modified value.
/// - `Err(TError)` to signal a failure.
pub trait Pipe<TPassable, TError> {
    /// Process the given `passable` value within this pipe.
    ///
    /// **Parameters**
    /// - `passable` - The current value flowing through the pipeline.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - `Ok(TPassable)`: The transformed or validated value to continue through the pipeline.
    /// - `Err(TError)`: An error indicating that this pipe failed and the pipeline should stop execution.
    fn handle(&self, passable: TPassable) -> Result<TPassable, TError>;
}

/// A single asynchronous processing unit within a [`AsyncPipeline`].
///
/// **Generics**
/// - `TPassable` - The type of the value that flows through the pipeline.
/// - `TError` - The error type returned when a pipe fails.
///
/// **Returns**
/// - `Ok(TPassable)` with the modified value.
/// - `Err(TError)` to signal a failure.
#[cfg(feature = "async")]
pub trait AsyncPipe<TPassable, TError> {
    /// Asynchronously process the given `passable` value within this pipe.
    ///
    /// **Parameters**
    /// - `passable` - The current value flowing through the pipeline.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` -The transformed or validated value to continue through the pipeline.
    /// - `Err(TError)` - An error indicating that this pipe failed and the pipeline should stop execution.
    fn handle<'a>(&'a self, passable: TPassable) -> Pin<Box<dyn Future<Output = Result<TPassable, TError>> + 'a>>;
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
    pipes: Vec<PipeType<TPassable, TError>>,

    /// A collection of observer closures (`Fn(&TPassable)`) that run
    /// after each successful pipe execution. These taps allow
    /// side effects such as logging, metrics, or debugging
    /// without modifying the pipeline value itself.
    #[cfg(feature = "taps")]
    taps: Vec<Box<dyn Fn(&TPassable)>>,
}








impl<TPassable, TError: std::fmt::Debug> Pipeline<TPassable, TError> where PipelineError: From<TError> {
    /// Creates a new, empty pipeline instance.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - A fresh pipeline instance with no input and no pipes.
    /// - If the `taps` feature is enabled, initializes an empty taps vector.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError};
    ///
    /// // Pipe that uppercases the input
    /// struct UpperPipe;
    /// impl Pipe<String, PipelineError> for UpperPipe {
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(input.to_uppercase())
    ///     }
    /// }
    ///
    /// // Pipe that adds a debug prefix
    /// struct DebugPipe;
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", input))
    ///     }
    /// }
    ///
    /// // Pipe that appends a suffix
    /// struct SuffixPipe;
    /// impl Pipe<String, PipelineError> for SuffixPipe {
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(format!("{}!!!", input))
    ///     }
    /// }
    ///
    /// fn main() {
    ///     // Create a new pipeline
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         // Provide initial input
    ///         .send("hello".to_string())
    ///         // Add UpperPipe only if condition is true
    ///         .when(true, Box::new(UpperPipe))
    ///         // Add DebugPipe only if condition is false
    ///         .unless(false, Box::new(DebugPipe))
    ///         // Always run SuffixPipe through the pipeline
    ///         .through(Box::new(SuffixPipe))
    ///         // Execute pipeline and return result
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "[DEBUG] HELLO!!!");
    /// }
    /// ```
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),

            #[cfg(feature = "taps")]
            taps: Vec::new(),
        }
    }

    /// Provides the initial passable value to the pipeline.
    ///
    /// **Parameters**
    /// - `passable` - The initial value that will flow through the pipeline.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the initial passable value set.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError};
    ///
    /// // Define a simple pipe that converts input to uppercase
    /// struct UpperPipe;
    ///
    /// // Implement Pipe trait for UpperPipe
    /// impl Pipe<String, PipelineError> for UpperPipe {
    ///     // Transform input by converting to uppercase
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(input.to_uppercase())
    ///     }
    /// }
    ///
    /// // Entry point
    /// fn main() {
    ///     // Create pipeline and provide initial input
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add UpperPipe to transform the input
    ///         .when(true, Box::new(UpperPipe))
    ///         // Execute pipeline and return result
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "HELLO");
    /// }
    /// ```
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Intercepts errors and allows recovery via a closure.
    ///
    /// **Parameters**
    /// - `f` - A closure that takes a [`PipelineError`] and produces a fallback `TPassable`.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - `Ok(TPassable)`: Either the fully processed pipeline value or the recovered value from `f`.
    /// - `Err(PipelineError)`: Only if the initial input is missing.
    ///
    /// **Usage**
    /// ```rust
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError, StepFailure};
    ///
    /// // Pipe that fails intentionally
    /// struct FailingPipe;
    /// impl Pipe<String, PipelineError> for FailingPipe {
    ///     fn handle(&self, _input: String) -> Result<String, PipelineError> {
    ///         Err(PipelineError::StepFailure(StepFailure {
    ///             step: "FailingPipe",
    ///             message: "Intentional failure".to_string()
    ///         }))
    ///     }
    /// }
    ///
    /// // Pipe that uppercases the input
    /// struct UpperPipe;
    /// impl Pipe<String, PipelineError> for UpperPipe {
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(input.to_uppercase())
    ///     }
    /// }
    ///
    /// fn main() {
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         .through(vec![Box::new(FailingPipe)]) // must wrap in vec![...]
    ///         .through(vec![Box::new(UpperPipe)])   // also wrap in vec![...]
    ///         .rescue(|err| {
    ///             format!("[RECOVERED after {:?}]", err)
    ///         });
    ///
    ///     assert!(result.is_ok());
    ///     assert_eq!(
    ///         result.unwrap(),
    ///         "[RECOVERED after StepFailure { step: \"FailingPipe\", message: \"Intentional failure\" }]"
    ///     );
    /// }
    /// ```
    pub fn rescue<F>(self, f: F) -> PipelineResult<TPassable>
    where
        F: FnOnce(PipelineError) -> TPassable,
    {
        let mut passable = self.passable.ok_or(PipelineError::InputMissing)?;
        for pipe in &self.pipes {
            match pipe.handle(passable) {
                Ok(val) => passable = val,
                Err(err) => {
                    // Recovery closure is actually used here
                    let recovered = f(utility::step_failure_from::<TError, TPassable>(err).into());
                    return Ok(recovered);
                }
            }
        }
        Ok(passable)
    }

    #[cfg(feature = "taps")]
    pub fn tap<F>(mut self, f: F) -> Self
    where
        F: Fn(&TPassable) + 'static,
    {
        self.taps.push(Box::new(f));
        self
    }

    #[cfg(not(feature = "taps"))]
    pub fn tap<F>(self, f: F) -> Self
    where
        F: Fn(&TPassable) + 'static,
    {
        if let Some(ref passable) = self.passable {
            f(passable);
        }
        self
    }

    /// Adds a sequence of pipes to the pipeline.
    pub fn through(mut self, pipes: Vec<PipeType<TPassable, TError>>) -> Self {
        for step in pipes {
            self.pipes.push(step);
        }
        self
    }

    /// Executes the pipeline and applies a final transformation closure to the result.
    pub fn then<TTransform, TOutput>(self, transform: TTransform) -> PipelineResult<TOutput>
    where
        TTransform: FnOnce(TPassable) -> TOutput,
    {
        // Ensure we have an initial input; otherwise return InputMissing error.
        let mut passable = self.passable.ok_or(PipelineError::InputMissing)?;

        // Run all pipes sequentially.
        for step in &self.pipes {
            match step.handle(passable) {
                // update passable value.
                Ok(val) => passable = val,

                // wrap error into pipeline error and stop execution.
                Err(err) => {
                    return Err(utility::step_failure_from::<TError, TPassable>(err).into());
                }
            }
        }

        // Apply final closure `transform` to the result.
        Ok(transform(passable))
    }

    /// Finalizes the pipeline and returns the processed output.
    pub fn then_return(self) -> PipelineResult<TPassable> {
        // Ensure we actually have an input value; otherwise return InputMissing error.
        let mut passable = utility::require_passable(self.passable)?;

        // Iterate over all pipes in sequence.
        for step in &self.pipes {
            match step.handle(passable) {
                // update passable value.
                Ok(val) => {
                    passable = val;

                    // If taps feature is enabled, run all observer closures.
                    #[cfg(feature = "taps")]
                    {
                        utility::run_taps(&self.taps, &passable);
                    }
                }

                // wrap error into pipeline error and stop execution.
                Err(err) => {
                    return Err(utility::step_failure_from::<TError, TPassable>(err).into());
                }
            }
        }

        // All pipes succeeded → return final passable value.
        Ok(passable)
    }

    /// Adds a pipe that runs only if the given condition evaluates to `true`.
    ///
    /// **Parameters**
    /// - `condition` - A boolean flag; if `true`, the provided pipe will be added.
    /// - `pipe` - A pipeline unit implementing [`Pipe`] that should run only when the condition is true.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the conditional pipe included if `condition` is true.
    /// - Otherwise, the pipeline instance unchanged.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError};
    ///
    /// // Define a simple pipe that adds a debug prefix
    /// struct DebugPipe;
    ///
    /// // Implement Pipe trait for DebugPipe
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     // Transform input by prefixing "[DEBUG]"
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", input))
    ///     }
    /// }
    ///
    /// // Entry point
    /// fn main() {
    ///     // Create pipeline and provide initial input
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add DebugPipe because condition is true
    ///         .when(true, Box::new(DebugPipe))
    ///         // Execute pipeline and return result
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "[DEBUG] hello");
    /// }
    /// ```
    pub fn when(mut self, condition: bool, pipe: PipeType<TPassable, TError>) -> Self {
        if condition {
            self.pipes.push(pipe);
        }
        self
    }

    /// Adds a pipe that runs only if the given condition evaluates to `false`.
    ///
    /// **Parameters**
    /// - `condition` - A boolean flag; if `false`, the provided pipe will be added.
    /// - `pipe` - A pipeline unit implementing [`Pipe`] that should run only when the condition is false.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the conditional pipe included if `condition` is false.
    /// - Otherwise, the pipeline instance unchanged.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError};
    ///
    /// // Define a simple pipe that adds a debug prefix
    /// struct DebugPipe;
    ///
    /// // Implement Pipe trait for DebugPipe
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     // Transform input by prefixing "[DEBUG]"
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", input))
    ///     }
    /// }
    ///
    /// // Entry point
    /// fn main() {
    ///     // Create pipeline and provide initial input
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add DebugPipe because condition is false
    ///         .unless(false, Box::new(DebugPipe))
    ///         // Execute pipeline and return result
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "[DEBUG] hello");
    /// }
    /// ```
    pub fn unless(mut self, condition: bool, pipe: PipeType<TPassable, TError>) -> Self {
        if !condition {
            self.pipes.push(pipe);
        }
        self
    }
}
