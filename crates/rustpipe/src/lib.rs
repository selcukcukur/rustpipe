pub mod errors;
pub mod utility;
pub mod types;

#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;

pub use crate::errors::*;
pub use crate::types::*;

#[cfg(feature = "macros")]
pub use rustpipe_macros::pipe;

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
    /// - `TPassable` - The type of the passable value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - A fresh pipeline instance with no passable value and no pipes.
    /// - If the `taps` feature is enabled, initializes an empty taps vector.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use std::sync::Arc;
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError, pipe};
    ///
    /// // Define a simple pipe that adds a debug prefix
    /// #[pipe(String, PipelineError)]
    /// struct DebugPipe;
    ///
    /// // Implement Pipe trait for debug pipe
    /// impl DebugPipe {
    ///     // Transform passable value by prefixing "[DEBUG]"
    ///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", passable))
    ///     }
    /// }
    ///
    /// fn main() {
    ///     // Create a new pipeline instance
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         // Provide initial passable value
    ///         .send("hello".to_string())
    ///         // Add debug pipe because condition is true
    ///         .when(true, Arc::new(DebugPipe))
    ///         // Execute pipeline and return final passable value
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "[DEBUG] hello");
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

    /// Runs a finalizer closure regardless of success or failure.
    pub fn finally<TFinalizer>(self, f: TFinalizer) -> Self
    where
        TFinalizer: FnOnce(Result<&TPassable, &PipelineError>) + 'static,
    {
        // Wrap pipeline execution with finalizer
        let result = if let Some(passable) = &self.passable {
            Ok(passable)
        } else {
            Err(&PipelineError::InputMissing)
        };

        f(result);
        self
    }

    /// Intercepts errors and allows recovery via a closure.
    ///
    /// **Parameters**
    /// - `recovery`- A closure (`TFallback`) that takes a [`PipelineError`] and produces a fallback `TPassable`.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the passable value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    /// - `TFallback` - A closure type that maps a [`PipelineError`] into a fallback `TPassable`.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - Either the fully processed pipeline value or the recovered value from `f`.
    /// - `Err(PipelineError)` - Only if the initial passable value is missing.
    ///
    /// Usage:
    /// ```rust
    /// use std::sync::Arc;
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError, StepFailure};
    ///
    /// // Pipe that fails intentionally
    /// #[derive(Pipe)]
    /// struct FailingPipe;
    ///
    /// impl FailingPipe {
    ///     // Always return an error to simulate failure
    ///     fn handle(&self, _passable: String) -> Result<String, PipelineError> {
    ///         Err(PipelineError::StepFailure(StepFailure {
    ///             step: "FailingPipe",
    ///             message: "Intentional failure".to_string()
    ///         }))
    ///     }
    /// }
    ///
    /// // Pipe that uppercases the passable value
    /// #[derive(Pipe)]
    /// struct UpperPipe;
    /// impl Pipe<String, PipelineError> for UpperPipe {
    ///     // Transform passable value by converting to uppercase
    ///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
    ///         Ok(passable.to_uppercase())
    ///     }
    /// }
    ///
    /// fn main() {
    ///     // Create pipeline and provide initial passable value
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add failing pipe (will trigger error)
    ///         .through(vec![Arc::new(FailingPipe)])
    ///         // Add upper pipe (would run if no error)
    ///         .through(vec![Arc::new(UpperPipe)])
    ///         // Rescue closure recovers from error
    ///         .rescue(|err| {
    ///             format!("[RECOVERED after {:?}]", err)
    ///         });
    ///
    ///     // Ensure pipeline succeeded despite failure
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected recovery message
    ///     assert_eq!(
    ///         result.unwrap(),
    ///         "[RECOVERED after StepFailure { step: \"FailingPipe\", message: \"Intentional failure\" }]"
    ///     );
    /// }
    /// ```
    pub fn rescue<TFallback>(self, recovery: TFallback) -> PipelineResult<TPassable>
    where
        TFallback: FnOnce(PipelineError) -> TPassable,
    {
        // Ensure we have an initial passable value, otherwise return InputMissing error
        let mut passable = self.passable.ok_or(PipelineError::InputMissing)?;

        // Iterate over all pipes sequentially
        for pipe in &self.pipes {
            match pipe.handle(passable) {
                // update passable value
                Ok(val) => passable = val,

                // invoke recovery closure
                Err(err) => {
                    let recovered = recovery(utility::step_failure_from::<TError, TPassable>(err).into());
                    return Ok(recovered);
                }
            }
        }

        // return final passable value
        Ok(passable)
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

    /// Adds a sequence of pipes to the pipeline.
    ///
    /// **Parameters**
    /// - `pipes` - A vector of [`PipeType`] instances to be executed sequentially.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the passable value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the provided pipes appended in order.
    /// - If no pipes are provided, the pipeline remains unchanged.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use std::sync::Arc;
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError, PipeType};
    ///
    /// // Define a pipe that uppercases the passable value
    /// struct UpperPipe;
    /// impl Pipe<String, PipelineError> for UpperPipe {
    ///     // Transform passable value by converting to uppercase
    ///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
    ///         Ok(passable.to_uppercase())
    ///     }
    /// }
    ///
    /// // Define a pipe that adds a debug prefix
    /// struct DebugPipe;
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     // Transform passable value by prefixing "[DEBUG]"
    ///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", passable))
    ///     }
    /// }
    ///
    /// fn main() {
    ///     // Create pipeline and provide initial passable value
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add multiple pipes sequentially (UpperPipe then DebugPipe)
    ///         .through(vec![Arc::new(UpperPipe), Arc::new(DebugPipe)])
    ///         // Execute pipeline and return final passable value
    ///         .then_return();
    ///
    ///     // Ensure pipeline succeeded
    ///     assert!(result.is_ok());
    ///     // Verify output matches expected
    ///     assert_eq!(result.unwrap(), "[DEBUG] HELLO");
    /// }
    /// ```
    pub fn through(mut self, pipes: Vec<PipeType<TPassable, TError>>) -> Self {
        for step in pipes {
            self.pipes.push(step);
        }
        self
    }

    /// Adds a pipe that runs only if the given condition evaluates to `false`.
    ///
    /// **Parameters**
    /// - `condition` - A boolean flag; if `false`, the provided pipe will be added.
    /// - `pipe` - A [`PipeType`] that should run only when the condition is false.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the passable value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the conditional pipe included if `condition` is false.
    /// - Otherwise, the pipeline instance unchanged.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use std::sync::Arc;
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError, PipeType};
    ///
    /// // Define a simple pipe that adds a debug prefix
    /// struct DebugPipe;
    ///
    /// // Implement pipe trait for debug pipe
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     // Transform passable value by prefixing "[DEBUG]"
    ///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", passable))
    ///     }
    /// }
    ///
    /// // Entry point
    /// fn main() {
    ///     // Create pipeline and provide initial passable value
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add debug pipe because condition is false
    ///         .unless(false, Arc::new(DebugPipe))
    ///         // Execute pipeline and return final passable value
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

    /// Adds a pipe that runs only if the given condition evaluates to `true`.
    ///
    /// **Parameters**
    /// - `condition` - A boolean flag; if `true`, the provided pipe will be added.
    /// - `pipe` - A [`PipeType`] that should run only when the condition is true.
    ///
    /// **Generics**
    /// - `TPassable` - The type of the passable value that flows through the pipeline.
    /// - `TError` - The error type returned when a pipe fails.
    ///
    /// **Returns**
    /// - The pipeline instance with the conditional pipe included if `condition` is true.
    /// - Otherwise, the pipeline instance unchanged.
    ///
    /// **Usage**
    /// ```rust
    /// // Import pipeline types and traits
    /// use std::sync::Arc;
    /// use rustpipe::{Pipeline, Pipe, PipelineResult, PipelineError};
    ///
    /// // Define a simple pipe that adds a debug prefix
    /// struct DebugPipe;
    ///
    /// // Implement pipe trait for debug pipe
    /// impl Pipe<String, PipelineError> for DebugPipe {
    ///     // Transform input by prefixing "[DEBUG]"
    ///     fn handle(&self, input: String) -> Result<String, PipelineError> {
    ///         Ok(format!("[DEBUG] {}", input))
    ///     }
    /// }
    ///
    /// // Entry point
    /// fn main() {
    ///     // Create pipeline and provide initial passable value
    ///     let result: PipelineResult<String> = Pipeline::new()
    ///         .send("hello".to_string())
    ///         // Add Debug pipe because condition is true
    ///         .when(true, Arc::new(DebugPipe))
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
}
