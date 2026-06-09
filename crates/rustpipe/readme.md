> Composable pipelines in Rust — define steps, chain transformations, and handle errors with elegance.

**rustpipe** is a lightweight and extensible pipeline engine designed for building deterministic data 
flows through composable processing stages.

Each step receives ownership of the current state, transforms it, and forwards the result to 
the next stage — enabling predictable execution without macros, reflection, or runtime complexity.

Built with simplicity and extensibility in mind, **rustpipe** can be used for middleware systems,
validation layers, transformation workflows, event processing, and schema-driven architectures
while remaining fully idiomatic and framework-agnostic.

* **Composable pipeline stages** powered by the `Pipe` trait
* **Predictable sequential execution** with explicit data flow
* **Built-in error handling** and recovery mechanisms
* **Conditional stage execution** using `when` and `unless`
* **Pipeline observation hooks** through lightweight taps
* **Optional async support** behind feature flags
* **Fully generic architecture** over input and error types
* **Minimal core design** focused on extensibility and control

**rustpipe** focuses on providing a small and predictable execution foundation that higher-level 
systems can build upon without imposing architectural constraints.

## Installation

### Cargo

Add **rustpipe** with default features to your project

```bash
cargo add rustpipe
```

Add **rustpipe** with the `async` feature enabled

```bash
cargo add rustpipe --features async
```

### Manual

Add **rustpipe** with default features to your `Cargo.toml`

```toml
[dependencies]
rustpipe = "1"
```

Add **rustpipe** with the `async` feature enabled

```toml
[dependencies]
rustpipe = { version = "1", features = ["async"] }
```

## Security

If you think there is a security vulnerability in the **Rustpipe**, you can help resolve the
issue immediately by sending an e-mail to **Selçuk Çukur** at **<hello@selcukcukur.me>**. Please
do not publicly post security vulnerabilities.

## License

**Rustpipe** project is published as open source. The **[MIT License](license.md)** is used, which
is one of the well-known open source coding licenses. You can get detailed information about the license terms
by visiting the link below.

- **[MIT License](license.md)**
