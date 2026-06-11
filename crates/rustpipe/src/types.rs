use std::sync::Arc;

use crate::errors::PipelineError;

#[cfg(feature = "async")]
use std::future::Future;
#[cfg(feature = "async")]
use std::pin::Pin;

/// Convenient alias for top-level pipeline operations.
///
/// **Generics**
/// - `TPassable` - The successful output type produced by the pipeline.
///
/// **Returns**
/// - `Ok(TPassable)` - The pipeline completed successfully.
/// - `Err(PipelineError)` - The pipeline failed with a centralized error.
pub type PipelineResult<TPassable> = Result<TPassable, PipelineError>;

/// Alias for middleware pipe results.
///
/// **Generics**
/// - `TPassable` - The successful output type produced by a middleware pipe.
/// - `TError` - The concrete error type returned by the middleware pipe.
///
/// **Returns**
/// - `Ok(TPassable)` - The pipe completed successfully.
/// - `Err(TError)` - The pipe failed and stopped the middleware chain.
pub type PipeResult<TPassable, TError = PipelineError> = Result<TPassable, TError>;

/// Alias for transform pipe results.
///
/// **Generics**
/// - `TPassable` - The successful output type produced by a transform pipe.
/// - `TError` - The concrete error type returned by the transform pipe.
///
/// **Returns**
/// - `Ok(TPassable)` - The transform completed successfully.
/// - `Err(TError)` - The transform failed and stopped the transform pipeline.
pub type TransformPipeResult<TPassable, TError = PipelineError> = Result<TPassable, TError>;

/// A thread-safe, shareable middleware unit.
///
/// **Generics**
/// - `TPassable` - The type of the value flowing through the middleware pipeline.
/// - `TError` - The error type returned when the pipe fails.
pub type PipeType<TPassable, TError = PipelineError> =
    Arc<dyn Pipe<TPassable, TError> + Send + Sync>;

/// A thread-safe, shareable transform unit.
///
/// **Generics**
/// - `TPassable` - The type of the value flowing through the transform pipeline.
/// - `TError` - The error type returned when the transform fails.
pub type TransformPipeType<TPassable, TError = PipelineError> =
    Arc<dyn TransformPipe<TPassable, TError> + Send + Sync>;

/// Boxed finalizer callback used by pipeline implementations.
pub type Finalizer<TPassable> = Box<dyn Fn(&PipelineResult<TPassable>) + Send + Sync>;

/// Boxed tap callback used by the optional `taps` feature.
pub type Tap<TPassable> = Box<dyn Fn(&TPassable) + Send + Sync>;

/// The continuation object passed to middleware pipes.
///
/// `Next` is inspired by Laravel's pipeline middleware flow. A middleware may call
/// [`Next::handle`] to continue, return early to short-circuit, or modify the
/// returned value after the rest of the stack has completed.
///
/// **Generics**
/// - `TPassable` - The type of the value flowing through the middleware pipeline.
/// - `TError` - The error type returned when any pipe fails.
pub struct Next<'a, TPassable, TError = PipelineError> {
    pipes: &'a [PipeType<TPassable, TError>],
    destination: &'a dyn Fn(TPassable) -> PipeResult<TPassable, TError>,
}

impl<'a, TPassable, TError> Next<'a, TPassable, TError> {
    /// Creates a continuation for the remaining middleware stack.
    ///
    /// **Parameters**
    /// - `pipes` - The remaining middleware pipes to execute.
    /// - `destination` - The final closure called after all middleware has run.
    ///
    /// **Returns**
    /// - A `Next` value that can continue the middleware chain.
    pub(crate) fn new(
        pipes: &'a [PipeType<TPassable, TError>],
        destination: &'a dyn Fn(TPassable) -> PipeResult<TPassable, TError>,
    ) -> Self {
        Self { pipes, destination }
    }

    /// Continues the middleware chain with the given passable value.
    ///
    /// **Parameters**
    /// - `passable` - The value that should be passed to the next middleware.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - The remaining chain completed successfully.
    /// - `Err(TError)` - A later middleware or destination failed.
    pub fn handle(self, passable: TPassable) -> PipeResult<TPassable, TError> {
        if let Some((pipe, rest)) = self.pipes.split_first() {
            let next = Next::new(rest, self.destination);
            pipe.handle(passable, next)
        } else {
            (self.destination)(passable)
        }
    }
}

/// A middleware pipe that can decide whether and when to call the next step.
///
/// **Generics**
/// - `TPassable` - The type of the value flowing through the middleware pipeline.
/// - `TError` - The error type returned when this pipe fails.
pub trait Pipe<TPassable, TError = PipelineError> {
    /// Handles a passable value and optionally continues the middleware chain.
    ///
    /// **Parameters**
    /// - `passable` - The current value flowing through the pipeline.
    /// - `next` - The continuation for the remaining middleware stack.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - The middleware completed successfully.
    /// - `Err(TError)` - The middleware failed and stopped execution.
    fn handle(
        &self,
        passable: TPassable,
        next: Next<'_, TPassable, TError>,
    ) -> PipeResult<TPassable, TError>;
}

/// A simple transform pipe that receives and returns the passable value.
///
/// **Generics**
/// - `TPassable` - The type of the value flowing through the transform pipeline.
/// - `TError` - The error type returned when this transform fails.
pub trait TransformPipe<TPassable, TError = PipelineError> {
    /// Transforms the given passable value.
    ///
    /// **Parameters**
    /// - `passable` - The current value flowing through the transform pipeline.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - The transformed value.
    /// - `Err(TError)` - The transform failed and stopped execution.
    fn handle(&self, passable: TPassable) -> TransformPipeResult<TPassable, TError>;
}

/// A thread-safe, shareable asynchronous middleware unit.
#[cfg(feature = "async")]
pub type AsyncPipeType<TPassable, TError = PipelineError> =
    Arc<dyn AsyncPipe<TPassable, TError> + Send + Sync>;

/// A thread-safe, shareable asynchronous transform unit.
#[cfg(feature = "async")]
pub type AsyncTransformPipeType<TPassable, TError = PipelineError> =
    Arc<dyn AsyncTransformPipe<TPassable, TError> + Send + Sync>;

/// Boxed future returned by async pipe operations.
#[cfg(feature = "async")]
pub type AsyncPipeFuture<'a, TPassable, TError = PipelineError> =
    Pin<Box<dyn Future<Output = PipeResult<TPassable, TError>> + Send + 'a>>;

/// Destination callback used by asynchronous middleware continuations.
#[cfg(feature = "async")]
pub type AsyncDestination<'a, TPassable, TError = PipelineError> =
    dyn Fn(TPassable) -> AsyncPipeFuture<'a, TPassable, TError> + Sync + 'a;

/// The asynchronous continuation object passed to async middleware pipes.
#[cfg(feature = "async")]
pub struct AsyncNext<'a, TPassable, TError = PipelineError> {
    pipes: &'a [AsyncPipeType<TPassable, TError>],
    destination: &'a AsyncDestination<'a, TPassable, TError>,
}

#[cfg(feature = "async")]
impl<'a, TPassable, TError> AsyncNext<'a, TPassable, TError>
where
    TPassable: Send + 'a,
    TError: Send + 'a,
{
    /// Creates an asynchronous continuation for the remaining middleware stack.
    pub(crate) fn new(
        pipes: &'a [AsyncPipeType<TPassable, TError>],
        destination: &'a AsyncDestination<'a, TPassable, TError>,
    ) -> Self {
        Self { pipes, destination }
    }

    /// Continues the asynchronous middleware chain with the given passable value.
    pub fn handle(
        self,
        passable: TPassable,
    ) -> Pin<Box<dyn Future<Output = PipeResult<TPassable, TError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some((pipe, rest)) = self.pipes.split_first() {
                let next = AsyncNext::new(rest, self.destination);
                pipe.handle(passable, next).await
            } else {
                (self.destination)(passable).await
            }
        })
    }
}

/// An asynchronous middleware pipe that can decide whether to call the next step.
#[cfg(feature = "async")]
pub trait AsyncPipe<TPassable, TError = PipelineError> {
    /// Handles a passable value and optionally continues the async chain.
    fn handle<'a>(
        &'a self,
        passable: TPassable,
        next: AsyncNext<'a, TPassable, TError>,
    ) -> Pin<Box<dyn Future<Output = PipeResult<TPassable, TError>> + Send + 'a>>;
}

/// An asynchronous transform pipe that receives and returns the passable value.
#[cfg(feature = "async")]
pub trait AsyncTransformPipe<TPassable, TError = PipelineError> {
    /// Transforms the given passable value asynchronously.
    fn handle<'a>(
        &'a self,
        passable: TPassable,
    ) -> Pin<Box<dyn Future<Output = TransformPipeResult<TPassable, TError>> + Send + 'a>>;
}
