use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{Next, Pipe, PipeResult, PipeType, Pipeline};

struct Add(u64);

impl Pipe<u64> for Add {
    fn handle(&self, passable: u64, next: Next<'_, u64>) -> PipeResult<u64> {
        next.handle(passable.wrapping_add(self.0))
    }
}

struct WrapXor(u64);

impl Pipe<u64> for WrapXor {
    fn handle(&self, passable: u64, next: Next<'_, u64>) -> PipeResult<u64> {
        let value = next.handle(passable)?;
        Ok(value ^ self.0)
    }
}

struct StopAfter(u64);

impl Pipe<u64> for StopAfter {
    fn handle(&self, passable: u64, _next: Next<'_, u64>) -> PipeResult<u64> {
        Ok(passable + self.0)
    }
}

fn pipes(count: usize) -> Vec<PipeType<u64>> {
    (0..count)
        .map(|index| {
            if index % 2 == 0 {
                Arc::new(Add(index as u64 + 1)) as PipeType<u64>
            } else {
                Arc::new(WrapXor(index as u64)) as PipeType<u64>
            }
        })
        .collect()
}

fn bench_sync_middleware_by_pipe_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_middleware/pipe_count");

    for pipe_count in [1usize, 10, 100, 1_000] {
        let stack = pipes(pipe_count);
        group.throughput(Throughput::Elements(pipe_count as u64));

        group.bench_with_input(
            BenchmarkId::new("u64_value", pipe_count),
            &stack,
            |b, stack| {
                b.iter(|| {
                    let output = Pipeline::new()
                        .send(black_box(0_u64))
                        .through(stack.clone())
                        .then_return()
                        .unwrap();

                    black_box(output);
                });
            },
        );
    }

    group.finish();
}

fn bench_sync_middleware_short_circuit(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_middleware/short_circuit");
    let tail = pipes(1_000);

    group.bench_function("stop_before_1000_pipe_tail", |b| {
        b.iter(|| {
            let mut stack: Vec<PipeType<u64>> = vec![Arc::new(StopAfter(10))];
            stack.extend(tail.clone());

            let output = Pipeline::new()
                .send(black_box(1_u64))
                .through(stack)
                .then_return()
                .unwrap();

            black_box(output);
        });
    });

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(25)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(8))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_sync_middleware_by_pipe_count,
        bench_sync_middleware_short_circuit
}

criterion_main!(benches);
