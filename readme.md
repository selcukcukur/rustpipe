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

**rustpipe** is a framework-agnostic pipeline crate for building clear, composable data flows. It
separates two common needs cleanly:

* **`Pipeline`** is Laravel-inspired middleware. A pipe receives `Next`, so it can continue,
  short-circuit, wrap downstream output, or return an error.
* **`TransformPipeline`** is a direct sequential transformer. A pipe receives a value and returns
  the next value without continuation/carry behavior.

The crate supports sync by default, async through a feature flag, optional taps, optional proc
macros, centralized `PipelineError` handling, integration tests, large-pipeline Criterion
benchmarks, and focused Ubuntu-based CI/CD workflows.

## Features

* Type-safe sync middleware with `Pipe`, `Next`, and `Pipeline`
* Type-safe sync transforms with `TransformPipe` and `TransformPipeline`
* Optional async API with `AsyncPipeline` and `AsyncTransformPipeline`
* Conditional composition through `when` and `unless`
* Recovery/finalization through `rescue` and `finally`
* Optional `taps` observers
* Optional `macros` attributes: `#[pipe]` and `#[transform_pipe]`
* Flexible errors: custom pipe errors can convert into `PipelineError`
* Stress benchmarks in `crates/rustpipe/benches/*`, split by sync, async, transform, middleware,
  and full pipeline workloads
* Functional tests in `crates/rustpipe/tests/*`, including 1000-item stress coverage
* Runnable crate examples in `crates/rustpipe/examples/*`
* Workspace rustfmt configuration through `rustfmt.toml`
* Coverage reporting through cargo-tarpaulin and Codecov

## Installation

```bash
cargo add rustpipe
```

```toml
[dependencies]
rustpipe = "1"
```

Feature examples:

```toml
[dependencies]
rustpipe = { version = "1", features = ["async"] }
rustpipe = { version = "1", features = ["taps"] }
rustpipe = { version = "1", features = ["macros"] }
rustpipe = { version = "1", features = ["async", "taps", "macros"] }
```

## Quickstart

### Middleware Pipeline

Use `Pipeline` when a step must decide whether the rest of the chain should run.

```rust
use std::sync::Arc;
use rustpipe::{Next, Pipe, PipeResult, Pipeline};

struct Prefix;

impl Pipe<String> for Prefix {
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
    .through(vec![Arc::new(Wrap), Arc::new(Prefix)])
    .then_return()?;

assert_eq!(output, "[app:hello]");
# Ok::<(), rustpipe::PipelineError>(())
```

### Transform Pipeline

Use `TransformPipeline` when every step is a normal input-to-output transform.

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

### `Pipeline<TPassable, TError = PipelineError>`

Laravel-style middleware pipeline.

* `new()` creates an empty pipeline.
* `send(passable)` sets the initial value.
* `through(Vec<PipeType<T, E>>)` appends middleware in order.
* `when(condition, pipe)` appends when `condition` is `true`.
* `unless(condition, pipe)` appends when `condition` is `false`.
* `tap(callback)` observes values; stored execution taps require the `taps` feature.
* `finally(callback)` runs after success or failure with `&PipelineResult<T>`.
* `then(destination)` runs the chain and maps the final value.
* `then_return()` runs the chain and returns the final value.
* `rescue(recovery)` converts a pipeline error into a fallback value.

### `Pipe<TPassable, TError = PipelineError>`

```rust
fn handle(
    &self,
    passable: TPassable,
    next: Next<'_, TPassable, TError>,
) -> PipeResult<TPassable, TError>;
```

Call `next.handle(passable)` to continue. Return `Ok(value)` to stop successfully, or `Err(error)`
to stop with an error.

### `Next<'a, TPassable, TError = PipelineError>`

* `handle(passable)` continues with the next middleware or final destination.

### `TransformPipeline<TPassable, TError = PipelineError>`

Sequential transform pipeline.

* `new()` creates an empty transform pipeline.
* `send(passable)` sets the initial value.
* `through(Vec<TransformPipeType<T, E>>)` appends transforms in order.
* `when(condition, pipe)` appends when `condition` is `true`.
* `unless(condition, pipe)` appends when `condition` is `false`.
* `tap(callback)` observes values.
* `finally(callback)` runs after success or failure with `&PipelineResult<T>`.
* `then(destination)` runs transforms and maps the final value.
* `then_return()` runs transforms and returns the final value.
* `rescue(recovery)` converts a transform error into a fallback value.

### `TransformPipe<TPassable, TError = PipelineError>`

```rust
fn handle(&self, passable: TPassable) -> TransformPipeResult<TPassable, TError>;
```

Return `Ok(value)` to continue or `Err(error)` to stop execution.

### Async API

Enable `async` for:

* `AsyncPipeline`
* `AsyncPipe`
* `AsyncNext`
* `AsyncTransformPipeline`
* `AsyncTransformPipe`
* `AsyncPipeType`
* `AsyncTransformPipeType`

Async execution mirrors sync usage:

```rust
let value = AsyncPipeline::new()
    .send(value)
    .through(pipes)
    .then_return()
    .await?;
```

### Result and Pipe Aliases

* `PipelineResult<T> = Result<T, PipelineError>`
* `PipeResult<T, E = PipelineError> = Result<T, E>`
* `TransformPipeResult<T, E = PipelineError> = Result<T, E>`
* `PipeType<T, E = PipelineError> = Arc<dyn Pipe<T, E> + Send + Sync>`
* `TransformPipeType<T, E = PipelineError> = Arc<dyn TransformPipe<T, E> + Send + Sync>`

### Errors

`PipelineError` variants:

* `StepFailure(StepFailure)`
* `InputMissing`
* `DispatchError(DispatchError)`
* `RescueFailure(RescueFailure)`
* `Custom(Box<dyn Error + Send + Sync>)`

Pipe errors may be custom types. Implement `From<YourError> for PipelineError` or
`Into<PipelineError>` to let them escape the pipeline cleanly.

### Macros

Enable `macros` for:

* `#[pipe(PassableType, ErrorType)]`
* `#[transform_pipe(PassableType, ErrorType)]`

## Benchmarks

```bash
cargo bench -p rustpipe
```

Benchmarks cover large transform pipelines, large middleware pipelines, short-circuit middleware,
async transform pipelines, async middleware pipelines, and a full 1000-item pipeline stress
workload.

Benchmark targets:

* `pipeline` - full middleware plus transform stress benchmark
* `sync_transform` - synchronous transform throughput
* `sync_middleware` - synchronous middleware throughput and short-circuit cost
* `async_transform` - asynchronous transform throughput
* `async_middleware` - asynchronous middleware throughput and short-circuit cost

## Examples

Runnable examples live in `crates/rustpipe/examples/*`.

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

## CI/CD

The verification workflows run for every push, every pull request, and manual dispatches:

* `.github/workflows/linting.yml` runs rustfmt, cargo check, clippy, and release builds.
* `.github/workflows/tests.yml` runs tests, all-features tests, doctests, and coverage reporting.
* `.github/workflows/benches.yml` compiles all benchmark targets.
* `.github/workflows/examples.yml` compiles all example targets.

`.github/workflows/publish.yml` runs only when a GitHub release is published. It verifies all
platforms and publishes crates when `CARGO_REGISTRY_TOKEN` is configured.

## Security

If you think there is a security vulnerability in **rustpipe**, please email **Selçuk Çukur** at
**<hello@selcukcukur.me>**. Please do not publicly post security vulnerabilities.

## License

**rustpipe** is open source under the **[MIT License](license.md)**.
