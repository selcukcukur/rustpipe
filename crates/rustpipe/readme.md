![Rustpipe Linting](https://github.com/selcukcukur/rustpipe/actions/workflows/linting.yml/badge.svg)
![Rustpipe Tests](https://github.com/selcukcukur/rustpipe/actions/workflows/tests.yml/badge.svg)
![Rustpipe Benches](https://github.com/selcukcukur/rustpipe/actions/workflows/benches.yml/badge.svg)
![Rustpipe Examples](https://github.com/selcukcukur/rustpipe/actions/workflows/examples.yml/badge.svg)
![Rustpipe Publish](https://github.com/selcukcukur/rustpipe/actions/workflows/publish.yml/badge.svg)
[![Coverage](https://codecov.io/gh/selcukcukur/rustpipe/branch/main/graph/badge.svg)](https://codecov.io/gh/selcukcukur/rustpipe)
[![Crates.io](https://img.shields.io/crates/v/rustpipe.svg)](https://crates.io/crates/rustpipe)
[![Docs.rs](https://docs.rs/rustpipe/badge.svg)](https://docs.rs/rustpipe)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](license.md)
![Rust](https://img.shields.io/badge/rust-2024-orange.svg)
![Rustfmt](https://img.shields.io/badge/code%20style-rustfmt-brightgreen.svg)
![Clippy](https://img.shields.io/badge/lints-clippy%20clean-brightgreen.svg)
![Tests](https://img.shields.io/badge/tests-sync%20%2B%20async%20%2B%20macros-brightgreen.svg)

> Type-safe middleware and transform pipelines for Rust.

**rustpipe** is a small, framework-agnostic pipeline crate for building clear data flows without
giving up control over execution. It gives you two complementary models:

* **`Pipeline`** for Laravel-inspired middleware. Each pipe receives a `Next` continuation, so it
  can continue the chain, wrap the downstream result, short-circuit execution, or return an error.
* **`TransformPipeline`** for direct sequential transforms. Each transform receives the current
  value and returns the next value without middleware carry/continuation behavior.

Both models are fully generic over the passable value and pipe error type. Public execution methods
return the centralized `PipelineError`, while individual pipes may use their own error types as long
as they can be converted into `PipelineError`.

## Features

* Type-safe sync middleware pipelines with `Pipe`, `Next`, and `Pipeline`
* Type-safe sync transform pipelines with `TransformPipe` and `TransformPipeline`
* Optional async middleware and transform pipelines behind the `async` feature
* Conditional composition with `when` and `unless`
* Recovery and finalization with `rescue` and `finally`
* Optional observation hooks through the `taps` feature
* Optional proc macros behind the `macros` feature
* Centralized error handling through `PipelineError`
* Criterion benchmarks split by sync, async, transform, middleware, and full pipeline scenarios
* Focused Ubuntu-based CI workflows
* Workspace rustfmt configuration through `rustfmt.toml`
* Coverage reporting through cargo-tarpaulin and Codecov
* Runnable crate examples for web adapters, validation, async jobs, and wgpu-style GPU pipelines

## Installation

### Default

```bash
cargo add rustpipe
```

```toml
[dependencies]
rustpipe = "1"
```

### Async Pipelines

```bash
cargo add rustpipe --features async
```

```toml
[dependencies]
rustpipe = { version = "1", features = ["async"] }
```

### Taps

```toml
[dependencies]
rustpipe = { version = "1", features = ["taps"] }
```

### Proc Macros

```toml
[dependencies]
rustpipe = { version = "1", features = ["macros"] }
```

### Everything

```toml
[dependencies]
rustpipe = { version = "1", features = ["async", "taps", "macros"] }
```

## Quickstart

### Best Practice: Middleware Pipeline

Use `Pipeline` when a step needs middleware behavior: authorization, validation, logging,
short-circuiting, before/after wrapping, retries, or anything that must decide whether the next
step should run.

```rust
use std::sync::Arc;
use rustpipe::{Next, Pipe, PipeResult, Pipeline};

struct AddPrefix;

impl Pipe<String> for AddPrefix {
    fn handle(&self, passable: String, next: Next<'_, String>) -> PipeResult<String> {
        next.handle(format!("app:{passable}"))
    }
}

struct Wrap;

impl Pipe<String> for Wrap {
    fn handle(&self, passable: String, next: Next<'_, String>) -> PipeResult<String> {
        let value = next.handle(passable)?;
        Ok(format!("[{value}]"))
    }
}

let output = Pipeline::new()
    .send("hello".to_string())
    .through(vec![Arc::new(Wrap), Arc::new(AddPrefix)])
    .then_return()?;

assert_eq!(output, "[app:hello]");
# Ok::<(), rustpipe::PipelineError>(())
```

### Best Practice: Transform Pipeline

Use `TransformPipeline` when every step is a pure “input to output” transform and does not need to
control the remaining chain.

```rust
use std::sync::Arc;
use rustpipe::{TransformPipe, TransformPipeResult, TransformPipeline};

struct Upper;

impl TransformPipe<String> for Upper {
    fn handle(&self, passable: String) -> TransformPipeResult<String> {
        Ok(passable.to_uppercase())
    }
}

let output = TransformPipeline::new()
    .send("hello".to_string())
    .through(vec![Arc::new(Upper)])
    .then_return()?;

assert_eq!(output, "HELLO");
# Ok::<(), rustpipe::PipelineError>(())
```

## API

### Core Types

#### `Pipeline<TPassable, TError = PipelineError>`

Middleware pipeline. Use it when a pipe must receive `Next`.

Methods:

* `Pipeline::new() -> Self` creates an empty pipeline.
* `.send(passable) -> Self` sets the initial value.
* `.through(Vec<PipeType<TPassable, TError>>) -> Self` appends middleware pipes in order.
* `.when(condition, pipe) -> Self` appends `pipe` only when `condition` is `true`.
* `.unless(condition, pipe) -> Self` appends `pipe` only when `condition` is `false`.
* `.tap(callback) -> Self` observes values. With `taps`, callbacks are stored and run during
  execution; without it, the callback observes the current sent value immediately.
* `.finally(callback) -> Self` runs after success or failure and receives `&PipelineResult<T>`.
* `.then(destination) -> PipelineResult<R>` executes the chain and maps the final value.
* `.then_return() -> PipelineResult<TPassable>` executes the chain and returns the final value.
* `.rescue(recovery) -> PipelineResult<TPassable>` recovers from pipeline errors with a fallback
  value. Missing input still returns `PipelineError::InputMissing`.

#### `Pipe<TPassable, TError = PipelineError>`

Middleware trait.

```rust
fn handle(
    &self,
    passable: TPassable,
    next: Next<'_, TPassable, TError>,
) -> PipeResult<TPassable, TError>;
```

Parameters:

* `passable` is the current value.
* `next` is the continuation for the remaining middleware stack.

Return `next.handle(passable)` to continue, return `Ok(value)` to short-circuit successfully, or
return `Err(error)` to stop with an error.

#### `Next<'a, TPassable, TError = PipelineError>`

Continuation passed to middleware.

* `.handle(passable) -> PipeResult<TPassable, TError>` executes the next middleware or the final
  destination if no middleware remains.

#### `TransformPipeline<TPassable, TError = PipelineError>`

Sequential transform pipeline. Use it when pipes do not need `Next`.

Methods:

* `TransformPipeline::new() -> Self` creates an empty transform pipeline.
* `.send(passable) -> Self` sets the initial value.
* `.through(Vec<TransformPipeType<TPassable, TError>>) -> Self` appends transforms in order.
* `.when(condition, pipe) -> Self` appends `pipe` only when `condition` is `true`.
* `.unless(condition, pipe) -> Self` appends `pipe` only when `condition` is `false`.
* `.tap(callback) -> Self` observes values.
* `.finally(callback) -> Self` runs after success or failure and receives `&PipelineResult<T>`.
* `.then(destination) -> PipelineResult<R>` executes transforms and maps the final value.
* `.then_return() -> PipelineResult<TPassable>` executes transforms and returns the final value.
* `.rescue(recovery) -> PipelineResult<TPassable>` recovers from transform errors.

#### `TransformPipe<TPassable, TError = PipelineError>`

Transform trait.

```rust
fn handle(&self, passable: TPassable) -> TransformPipeResult<TPassable, TError>;
```

Parameters:

* `passable` is the current value.

Return `Ok(value)` to continue with the next transform or `Err(error)` to stop execution.

### Async API

Enable with:

```toml
rustpipe = { version = "1", features = ["async"] }
```

Types:

* `AsyncPipeline<TPassable, TError = PipelineError>`
* `AsyncPipe<TPassable, TError = PipelineError>`
* `AsyncNext<'a, TPassable, TError = PipelineError>`
* `AsyncTransformPipeline<TPassable, TError = PipelineError>`
* `AsyncTransformPipe<TPassable, TError = PipelineError>`

Async methods mirror the sync API where available:

* `AsyncPipeline::new().send(value).through(pipes).then_return().await`
* `AsyncPipeline::new().send(value).through(pipes).then(destination).await`
* `AsyncTransformPipeline::new().send(value).through(pipes).then_return().await`
* `AsyncTransformPipeline::new().send(value).through(pipes).then(destination).await`

### Result Aliases

* `PipelineResult<T> = Result<T, PipelineError>`
* `PipeResult<T, E = PipelineError> = Result<T, E>`
* `TransformPipeResult<T, E = PipelineError> = Result<T, E>`

### Shared Pipe Aliases

* `PipeType<T, E = PipelineError> = Arc<dyn Pipe<T, E> + Send + Sync>`
* `TransformPipeType<T, E = PipelineError> = Arc<dyn TransformPipe<T, E> + Send + Sync>`
* `AsyncPipeType<T, E = PipelineError> = Arc<dyn AsyncPipe<T, E> + Send + Sync>`
* `AsyncTransformPipeType<T, E = PipelineError> = Arc<dyn AsyncTransformPipe<T, E> + Send + Sync>`

### Errors

`PipelineError` is the public error returned by pipeline execution:

* `PipelineError::StepFailure(StepFailure)` for named step failures.
* `PipelineError::InputMissing` when `then`, `then_return`, or `rescue` is called before `send`.
* `PipelineError::DispatchError(DispatchError)` for dispatch-layer failures.
* `PipelineError::RescueFailure(RescueFailure)` for rescue-layer failures.
* `PipelineError::Custom(Box<dyn Error + Send + Sync>)` for external error types.

Custom pipe errors are supported by implementing `From<YourError> for PipelineError` or
`Into<PipelineError>` for the error type.

### Macros

Enable with:

```toml
rustpipe = { version = "1", features = ["macros"] }
```

Available attributes:

* `#[pipe(PassableType, ErrorType)]` implements middleware `Pipe`.
* `#[transform_pipe(PassableType, ErrorType)]` implements `TransformPipe`.

## Benchmarks

Benchmarks live under `benches/*` and cover:

* `pipeline` - full middleware plus transform stress benchmark over 1000 items,
* `sync_transform` - synchronous transform throughput,
* `sync_middleware` - synchronous middleware throughput and short-circuit cost,
* `async_transform` - asynchronous transform throughput,
* `async_middleware` - asynchronous middleware throughput and short-circuit cost.

Run them with:

```bash
cargo bench -p rustpipe
```

## Examples

Runnable examples live under `examples/*`.

```bash
cargo run -p rustpipe --example basic_transform
cargo run -p rustpipe --example middleware_auth
cargo run -p rustpipe --example axum_adapter
cargo run -p rustpipe --example actix_web_adapter
cargo run -p rustpipe --example data_validation
cargo run -p rustpipe --example gpu_wgpu_pipeline
cargo run -p rustpipe --features async --example async_jobs
```

The web and GPU examples use framework-shaped adapter types instead of forcing heavy framework or
GPU dependencies into the crate. They show how to place rustpipe around Axum-like handlers,
Actix-like service requests, validation flows, async jobs, and wgpu-style render command pipelines.

## Security

If you think there is a security vulnerability in **rustpipe**, please email
**Selçuk Çukur** at **<hello@selcukcukur.me>**. Please do not publicly post security
vulnerabilities.

## License

**rustpipe** is open source under the **[MIT License](license.md)**.
