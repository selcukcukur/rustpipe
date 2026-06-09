# RustPipe

## About

**RustPipe** is a lightweight yet powerful framework for building composable data pipelines in Rust. 
It lets you define reusable **Pipe steps**, chain them seamlessly with **Pipeline**, and handle errors 
through categorized **PipelineError**.

### Features
- **Composable steps** — implement the `Pipe` trait to transform or validate input
- **Ergonomic chaining** — `send → through → tap → then/then_return → rescue`
- **Error handling** — human‑readable messages via `Display`, automatic conversions via `From`
- **Conditional execution** — run steps only when needed with **when** / **unless**
- **Rescue support** — recover gracefully from failures with custom fallback logic
