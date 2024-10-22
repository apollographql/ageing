use priority_queue::PriorityQueue as OtherPriorityQueue;
use rand::distributions::Uniform;
use rand::Rng;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BatchSize, BenchmarkId};

use ageing::Priority;
use ageing::PriorityQueue;

pub fn priority_queue(c: &mut Criterion) {
    const KB: usize = 1024;
    let mut rng = rand::thread_rng();
    let range = Uniform::new(0, 8);

    let mut group = c.benchmark_group("enqueue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                // || -> (PriorityQueue<()>, Vec<(Priority, BoxedTask<()>)>) {
                || -> (PriorityQueue<()>, Vec<(Priority, _)>) {
                    let queue = PriorityQueue::bounded(Some(*limit));
                    // let mut tasks: Vec<(Priority, BoxedTask<()>)> = Vec::with_capacity(*limit);
                    let mut tasks: Vec<(Priority, _)> = Vec::with_capacity(*limit);

                    for _ in 0..*limit {
                        let priority_val = rng.sample(range) as u8;
                        let priority =
                            Priority::try_from(priority_val).expect("it's a valid priority");
                        tasks.push((priority, || {}));
                    }
                    (queue, tasks)
                },
                |(queue, tasks)| {
                    let (priority, task) = tasks.pop().expect("we have enough tasks");
                    queue
                        .enqueue(priority, task)
                        .expect("we can enqueue our task");
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    let mut group = c.benchmark_group("random dequeue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                || -> PriorityQueue<()> {
                    let mut queue = PriorityQueue::bounded(Some(*limit));

                    for _ in 0..*limit {
                        let priority_val = rng.sample(range) as u8;
                        let priority =
                            Priority::try_from(priority_val).expect("it's a valid priority");
                        queue
                            .enqueue(priority, || {})
                            .expect("we can enqueue our task");
                    }
                    queue
                },
                |queue| {
                    let _ = queue.dequeue().expect("we can dequeue a task");
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();

    let mut group = c.benchmark_group("fastest dequeue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                || -> PriorityQueue<()> {
                    let mut queue = PriorityQueue::bounded(Some(*limit));

                    for _ in 0..*limit {
                        queue
                            .enqueue(Priority::Zero, || {})
                            .expect("we can enqueue our task");
                    }
                    queue
                },
                |queue| {
                    let _ = queue.dequeue().expect("we can dequeue a task");
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();

    let mut group = c.benchmark_group("slowest dequeue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                || -> PriorityQueue<()> {
                    let mut queue = PriorityQueue::bounded(Some(*limit));

                    for _ in 0..*limit {
                        queue
                            .enqueue(Priority::Seven, || {})
                            .expect("we can enqueue our task");
                    }
                    queue
                },
                |queue| {
                    let _ = queue.dequeue().expect("we can dequeue a task");
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

pub fn other_priority_queue(c: &mut Criterion) {
    const KB: usize = 1024;
    let mut rng = rand::thread_rng();
    let range = Uniform::new(0, 8);

    let mut group = c.benchmark_group("other_enqueue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                || -> (OtherPriorityQueue<String, u8>, Vec<(u8, String)>) {
                    let queue = OtherPriorityQueue::new();
                    let mut tasks: Vec<(u8, String)> = Vec::with_capacity(*limit);

                    for _ in 0..*limit {
                        let priority_val = rng.sample(range) as u8;
                        tasks.push((priority_val, priority_val.to_string()));
                    }
                    (queue, tasks)
                },
                |(queue, tasks)| {
                    let (priority, task) = tasks.pop().expect("we have enough tasks");
                    // queue.push(task, priority).expect("we can enqueue our task");
                    queue.push(task, priority);
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();

    let mut group = c.benchmark_group("other_dequeue");

    for limit in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB].iter() {
        group.bench_function(BenchmarkId::new("limit", limit), |b| {
            b.iter_batched_ref(
                || -> OtherPriorityQueue<String, u8> {
                    let mut queue = OtherPriorityQueue::new();

                    for _ in 0..*limit {
                        let priority_val = rng.sample(range) as u8;
                        queue.push(priority_val.to_string(), priority_val);
                    }
                    queue
                },
                |queue| {
                    let _ = queue.pop().expect("we can dequeue a task");
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, priority_queue, other_priority_queue);
criterion_main!(benches);
