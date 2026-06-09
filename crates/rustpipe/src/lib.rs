#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;

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
    method: String,
}

impl<T, E> Pipeline<T, E> {
    /// Creates a new, empty pipeline instance.
    ///
    /// # Behavior
    /// - Initializes the pipeline with no input (`input = None`).
    /// - Starts with an empty list of steps (`steps = Vec::new()`).
    /// - Sets the default method name to `"handle"`.
    /// - This method is typically the first call when constructing a pipeline,
    ///   followed by [`Pipeline::send`] to provide input and [`Pipeline::through`]
    ///   to add steps.
    ///
    /// # Return
    /// - Returns a fresh [`Pipeline`] instance ready for configuration.
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
    /// fn main() {
    ///     let pipeline = Pipeline::new()
    ///         .send("   hello rustpipe   ".to_string())
    ///         .through(vec![Box::new(TrimStep)]);
    ///
    ///     let result = pipeline.then_return();
    ///     assert_eq!(result.unwrap(), "hello rustpipe");
    /// }
    /// ```
    pub fn new() -> Self {
        Self {
            input: None,
            steps: Vec::new(),
            method: "handle".to_string(),
        }
    }

    /// Provides the initial input value to the pipeline.
    ///
    /// # Behavior
    /// - Consumes the pipeline instance.
    /// - Stores the given input inside the pipeline state.
    /// - This input will later be passed sequentially through all steps
    ///   added via [`Pipeline::through`].
    /// - Must be called before [`Pipeline::then_return`] or [`Pipeline::then`],
    ///   otherwise those methods will panic due to missing input.
    ///
    /// # Parameters
    /// - `input`: The initial value of type `T` to be processed by the pipeline.
    ///
    /// # Return
    /// - Returns the pipeline instance with the input set, enabling further chaining.
    ///
    /// # Panics
    /// - Does not panic. However, if this method is not called before finalizing
    ///   the pipeline, subsequent methods will panic when attempting to access input.
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
    /// fn main() {
    ///     let result = Pipeline::new()
    ///         .send("   hello rustpipe   ".to_string()) // provide input
    ///         .through(vec![Box::new(TrimStep)])
    ///         .then_return();
    ///
    ///     assert_eq!(result.unwrap(), "hello rustpipe");
    /// }
    /// ```
    pub fn send(mut self, input: T) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the method name to be used when invoking pipeline steps.
    ///
    /// # Behavior
    /// - Consumes the pipeline instance.
    /// - Stores the provided method name as a string in the pipeline state.
    /// - Intended for customizing how steps are dispatched, allowing flexibility
    ///   if different step types expose multiple handler methods.
    /// - By default, steps are expected to implement [`Pipe`] with a `handle` method.
    ///   Using `via` makes it possible to switch to another method name if supported.
    ///
    /// # Parameters
    /// - `method`: The name of the method to call on each step (e.g., `"handle"`, `"process"`).
    ///
    /// # Return
    /// - Returns the pipeline instance with the updated method setting, enabling further chaining.
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
    /// fn main() {
    ///     let result = Pipeline::new()
    ///         .send("   hello rustpipe   ".to_string())
    ///         .through(vec![Box::new(TrimStep)])
    ///         .via("handle") // specify which method to call
    ///         .then_return();
    ///
    ///     assert_eq!(result.unwrap(), "hello rustpipe");
    /// }
    /// ```
    pub fn via(mut self, method: &str) -> Self {
        self.method = method.to_string();
        self
    }

    /// Observes the current pipeline input without modifying it.
    ///
    /// # Behavior
    /// - Consumes the pipeline instance.
    /// - If an input has been provided via [`Pipeline::send`], the closure `f` is invoked
    ///   with a reference to that input.
    /// - The closure can be used for logging, debugging, or side effects.
    /// - The pipeline state remains unchanged; the input is not modified.
    ///
    /// # Parameters
    /// - `f`: A closure that takes a reference to the current input value (`&T`).
    ///
    /// # Return
    /// - Returns the pipeline instance unchanged, allowing further chaining.
    ///
    /// # Panics
    /// - Does not panic. If no input is set, the closure is simply not invoked.
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
    /// fn main() {
    ///     let result = Pipeline::new()
    ///         .send("   hello rustpipe   ".to_string())
    ///         .through(vec![Box::new(TrimStep)])
    ///         .tap(|val| println!("Before processing: {}", val))
    ///         .then_return();
    ///
    ///     assert_eq!(result.unwrap(), "hello rustpipe");
    /// }
    /// ```
    pub fn tap<F>(self, f: F) -> Self
    where
        F: Fn(&T) + 'static,
    {
        if let Some(ref input) = self.input {
            f(input);
        }
        self
    }

    /// Adds a sequence of steps to the pipeline.
    ///
    /// # Behavior
    /// - Consumes the provided vector of steps.
    /// - Each step must implement the [`Pipe`] trait and be wrapped in a `Box<dyn Pipe<T, E>>`.
    /// - Steps are appended to the pipeline in the order they appear in the vector.
    /// - The pipeline can then execute these steps sequentially when [`Pipeline::then_return`] or [`Pipeline::then`] is called.
    ///
    /// # Parameters
    /// - `steps`: A vector of boxed step instances implementing [`Pipe<T, E>`].
    ///
    /// # Return
    /// - Returns the updated pipeline instance with the new steps included.
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
    pub fn through(mut self, steps: Vec<Box<dyn Pipe<T, E>>>) -> Self {
        for step in steps {
            self.steps.push(step);
        }
        self
    }

    /// Executes the pipeline and applies a final transformation closure to the result.
    ///
    /// # Behavior
    /// - Consumes the pipeline instance.
    /// - Takes the input previously provided with [`Pipeline::send`].
    /// - Sequentially applies each step added with [`Pipeline::through`].
    /// - Each step must implement the [`Pipe`] trait and return a `Result<T, E>`.
    /// - After all steps succeed, the final value is passed into the provided closure `f`.
    ///
    /// # Return
    /// - `Ok(R)` if all steps succeed and the closure produces a result.
    /// - `Err(E)` if any step fails; execution stops immediately at the failing step.
    ///
    /// # Panics
    /// - If no input was provided before calling this method, it will panic with
    ///   `"Pipeline input not set"`.
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
    ///         .then(|val| format!("Final result: {}", val));
    ///
    ///     assert_eq!(result.unwrap(), "Final result: HELLO RUSTPIPE");
    /// }
    /// ```
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

    /// Finalizes the pipeline and returns the processed output.
    ///
    /// # Behavior
    /// - Consumes the pipeline instance.
    /// - Takes the input previously provided with [`Pipeline::send`].
    /// - Sequentially applies each step added with [`Pipeline::through`].
    /// - Each step must implement the [`Pipe`] trait and return a `Result<T, E>`.
    ///
    /// # Return
    /// - `Ok(T)` if all steps succeed and produce a final value.
    /// - `Err(E)` if any step fails; execution stops immediately at the failing step.
    ///
    /// # Panics
    /// - If no input was provided before calling this method, it will panic with
    ///   `"Pipeline input not set"`.
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
    pub fn then_return(self) -> Result<T, E> {
        let mut input = self.input.expect("Pipeline input not set");
        for step in &self.steps {
            input = step.handle(input)?;
        }
        Ok(input)
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