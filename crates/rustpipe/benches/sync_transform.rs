use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{TransformPipe, TransformPipeResult, TransformPipeType, TransformPipeline};

struct Add(u64);

impl TransformPipe<Vec<u64>> for Add {
    fn handle(&self, mut passable: Vec<u64>) -> TransformPipeResult<Vec<u64>> {
        for value in &mut passable {
            *value = value.wrapping_add(self.0);
        }

        Ok(passable)
    }
}

struct Mix;

impl TransformPipe<Vec<u64>> for Mix {
    fn handle(&self, mut passable: Vec<u64>) -> TransformPipeResult<Vec<u64>> {
        for value in &mut passable {
            *value = value.rotate_left(7) ^ 0x9E37_79B9;
        }

        Ok(passable)
    }
}

fn values(count: usize) -> Vec<u64> {
    (0..count as u64).collect()
}

fn pipes(count: usize) -> Vec<TransformPipeType<Vec<u64>>> {
    (0..count)
        .map(|index| {
            if index % 2 == 0 {
                Arc::new(Add(index as u64 + 1)) as TransformPipeType<Vec<u64>>
            } else {
                Arc::new(Mix) as TransformPipeType<Vec<u64>>
            }
        })
        .collect()
}

fn bench_sync_transform_by_pipe_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_transform/pipe_count");

    for pipe_count in [1usize, 10, 100, 1_000] {
        let stack = pipes(pipe_count);
        group.throughput(Throughput::Elements(pipe_count as u64));

        group.bench_with_input(
            BenchmarkId::new("1000_values", pipe_count),
            &stack,
            |b, stack| {
                b.iter(|| {
                    let output = TransformPipeline::new()
                        .send(black_box(values(1_000)))
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

fn bench_sync_transform_by_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_transform/batch_size");
    let stack = pipes(25);

    for size in [10usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("25_pipes", size), &size, |b, size| {
            b.iter(|| {
                let output = TransformPipeline::new()
                    .send(black_box(values(*size)))
                    .through(stack.clone())
                    .then_return()
                    .unwrap();

                black_box(output);
            });
        });
    }

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
        bench_sync_transform_by_pipe_count,
        bench_sync_transform_by_batch_size
}

criterion_main!(benches);
