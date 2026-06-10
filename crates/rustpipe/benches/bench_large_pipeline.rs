use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{
    criterion_group,
    criterion_main,
    BenchmarkId,
    Criterion,
    Throughput,
};

use rustpipe::{Pipe, Pipeline, PipelineError, PipelineResult};

/// ------------------------------------------------------------
/// PURE OVERHEAD PIPE
/// ------------------------------------------------------------

struct AddPipe;

impl Pipe<u64, PipelineError> for AddPipe {
    fn handle(&self, input: u64) -> PipelineResult<u64> {
        Ok(input + 1)
    }
}

/// ------------------------------------------------------------
/// STRING WORKLOAD PIPE
/// ------------------------------------------------------------

struct UpperPipe;

impl Pipe<String, PipelineError> for UpperPipe {
    fn handle(&self, input: String) -> PipelineResult<String> {
        Ok(input.to_uppercase())
    }
}

/// ------------------------------------------------------------
/// REUSABLE PIPELINES
/// ------------------------------------------------------------

fn build_u64_pipeline() -> Arc<Vec<Box<dyn Pipe<u64, PipelineError>>>> {
    Arc::new(vec![
        Box::new(AddPipe),
        Box::new(AddPipe),
        Box::new(AddPipe),
        Box::new(AddPipe),
        Box::new(AddPipe),
    ])
}

fn build_string_pipeline() -> Arc<Vec<Box<dyn Pipe<String, PipelineError>>>> {
    Arc::new(vec![
        Box::new(UpperPipe),
        Box::new(UpperPipe),
        Box::new(UpperPipe),
    ])
}

/// ------------------------------------------------------------
/// INPUT GENERATORS
/// ------------------------------------------------------------

fn generate_u64_inputs(size: usize) -> Vec<u64> {
    (0..size as u64).collect()
}

fn generate_string_inputs(size: usize) -> Vec<String> {
    (0..size)
        .map(|i| format!("data{}", i))
        .collect()
}

/// ------------------------------------------------------------
/// BENCHMARKS
/// ------------------------------------------------------------

fn bench_pure_pipeline_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_overhead");

    let sizes = [1_000usize, 10_000, 100_000];

    for size in sizes {
        let inputs = generate_u64_inputs(size);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("u64_pipeline", size),
            &inputs,
            |b, inputs| {
                let pipes = build_u64_pipeline();

                b.iter(|| {
                    for input in inputs {
                        let result = Pipeline::new()
                            .send(black_box(*input))
                            .through((*pipes))
                            .then_return();

                        black_box(result.unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_string_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_workload");

    let sizes = [1_000usize, 10_000, 100_000];

    for size in sizes {
        let inputs = generate_string_inputs(size);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("string_pipeline", size),
            &inputs,
            |b, inputs| {
                let pipes = build_string_pipeline();

                b.iter(|| {
                    for input in inputs.iter().cloned() {
                        let result = Pipeline::new()
                            .send(black_box(input))
                            .through((*pipes).clone())
                            .then_return();

                        black_box(result.unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

/// ------------------------------------------------------------
/// CRITERION CONFIG
/// ------------------------------------------------------------

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(100)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(15))
}

/// ------------------------------------------------------------
/// ENTRY
/// ------------------------------------------------------------

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_pure_pipeline_overhead,
        bench_string_workload
}

criterion_main!(benches);
