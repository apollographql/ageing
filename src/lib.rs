use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::mpsc::Sender;

use thiserror::Error;

mod scheduler;

type TaskType<T> = dyn FnOnce() -> T + Send + 'static;

pub type BoxedTask<T> = Box<TaskType<T>>;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("Queue is full.")]
    QueueFull,
    #[error("Invalid priority: {}.", 0)]
    InvalidPriority(u8),
    #[error("Notification send failed: {}.", 0)]
    SendFailed(String),
}

/// 0 is the highest priority, 7 is the lowest
#[derive(Debug, PartialEq)]
pub enum Priority {
    Zero = 0,
    One,
    Two,
    Three,
    Four,
    Five,
    Six,
    Seven,
}
impl TryFrom<u8> for Priority {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == Priority::Zero as u8 => Ok(Priority::Zero),
            x if x == Priority::One as u8 => Ok(Priority::One),
            x if x == Priority::Two as u8 => Ok(Priority::Two),
            x if x == Priority::Three as u8 => Ok(Priority::Three),
            x if x == Priority::Four as u8 => Ok(Priority::Four),
            x if x == Priority::Five as u8 => Ok(Priority::Five),
            x if x == Priority::Six as u8 => Ok(Priority::Six),
            x if x == Priority::Seven as u8 => Ok(Priority::Seven),
            _ => Err(Error::InvalidPriority(v)),
        }
    }
}

const PRIORITY_LEVELS: usize = 8;

#[derive(Default)]
pub struct PriorityQueue<T> {
    queues: [VecDeque<BoxedTask<T>>; PRIORITY_LEVELS],
    limit: Option<usize>,
    callbacks: VecDeque<Sender<BoxedTask<T>>>,
}

impl<T: Send + 'static> Debug for PriorityQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("PriorityQueue");
        s.field("limit", &self.limit);
        for idx in 0..PRIORITY_LEVELS {
            let field = format!("priority: {idx}, tasks");
            s.field(&field, &self.queues[idx].len());
        }
        s.finish()
    }
}

impl<T> PriorityQueue<T>
where
    T: Send + 'static,
{
    /// Create an unlimited queue
    pub fn new() -> Self {
        Self::bounded(None)
    }

    /// Create a limited queue
    pub fn bounded(limit: Option<usize>) -> Self {
        let queues: [VecDeque<BoxedTask<T>>; PRIORITY_LEVELS] =
            std::array::from_fn(|_i| VecDeque::new());

        Self {
            queues,
            limit,
            callbacks: VecDeque::new(),
        }
    }

    // Note: It would be simpler to just say len() != 0, but that would burn more CPU...
    /// Is our queue empty?
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// Get the number of queueing tasks
    pub fn len(&self) -> usize {
        self.queues.iter().fold(0, |acc, x| acc + x.len())
    }

    /// Get the number of queueing tasks at the specified priority
    pub fn len_with_priority(&self, priority: Priority) -> usize {
        self.queues[priority as usize].len()
    }

    /// Clear the queue
    pub fn clear(&mut self) {
        self.queues.iter_mut().for_each(|q| q.clear())
    }

    /// Clear the queue at the specified priority
    pub fn clear_with_priority(&mut self, priority: Priority) {
        self.queues[priority as usize].clear()
    }

    /// Drain the queue into an iterator
    pub fn drain(self) -> impl Iterator<Item = BoxedTask<T>> {
        self.queues.into_iter().flatten()
    }

    /// Enqueue a task
    pub fn enqueue(
        &mut self,
        priority: Priority,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> Result<(), Error> {
        let boxed_task: BoxedTask<T> = Box::new(task);
        // If we are empty and there is a registered waiter, don't queue just immediately notify
        if self.is_empty() {
            if let Some(tx) = self.callbacks.pop_front() {
                return tx
                    .send(boxed_task)
                    .map_err(|e| Error::SendFailed(e.to_string()));
            }
        }

        if let Some(limit) = self.limit {
            if self.len() == limit {
                return Err(Error::QueueFull);
            }
        }
        self.queues[priority as usize].push_back(boxed_task);
        Ok(())
    }

    /// Dequeue a task
    /// Algorithm: Find the highest priority task in our queue
    ///  - Search sequentially through priority levels for a task
    ///  - When we find one, that's our result
    ///    - If we found one, promote queue heads of all lower
    ///      priority tasks
    ///  - Return our result
    pub fn dequeue(&mut self) -> Option<BoxedTask<T>> {
        let mut result = None;
        for idx in 0..PRIORITY_LEVELS {
            if !self.queues[idx].is_empty() {
                result = self.queues[idx].pop_front();
                let limit = if idx == 0 { 1 } else { idx };
                for idx in limit..PRIORITY_LEVELS {
                    if let Some(elem) = self.queues[idx].pop_front() {
                        self.queues[idx - 1].push_back(elem);
                    }
                }
                break;
            }
        }
        result
    }

    /// Register a channel for notification of Task
    ///
    /// Note: This is inherently racy, so be aware that a notification won't be raised if a task is
    /// enqueue'd during registration. One way to avoid this problem is to synchronise around the
    /// entire queue:
    ///
    /// ```
    ///    use std::sync::Arc;
    ///    use std::sync::Mutex;
    ///    use std::sync::mpsc::channel;
    ///    use std::thread::sleep;
    ///    use std::time::Duration;
    ///
    ///    use ageing::Priority;
    ///    use ageing::PriorityQueue;
    ///
    ///    let queue = Arc::new(Mutex::new(PriorityQueue::bounded(Some(1))));
    ///
    ///      let my_queue = queue.clone();
    ///
    ///      let (tx, rx) = channel();
    ///
    ///      std::thread::spawn(move || {
    ///        let (tx_cb, rx_cb) = channel();
    ///        let mut guard = my_queue.lock().unwrap();
    ///
    ///        let mut task = guard.dequeue();
    ///
    ///        if task.is_none() {
    ///          guard.register(tx_cb);
    ///          drop(guard);
    ///          task = Some(rx_cb.recv().expect("to get a task at some point"));
    ///        }
    ///        tx.send(task).expect("we sent our task");
    ///      });
    ///
    ///      // Slight pause to allow other thread to register
    ///      sleep(Duration::from_millis(20));
    ///
    ///      let task = move || {};
    ///      queue
    ///        .lock()
    ///        .unwrap()
    ///        .enqueue(Priority::Zero, task)
    ///        .expect("we can enqueue our task");
    ///      let task = rx.recv().expect("to get a task at some point");
    ///
    ///      assert!(task.is_some())
    /// ```
    pub fn register(&mut self, tx: Sender<BoxedTask<T>>) {
        self.callbacks.push_back(tx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread::sleep;
    use std::time::Duration;

    use rand::distributions::Uniform;
    use rand::Rng;

    use super::*;

    #[test]
    fn it_will_not_enqueue_when_limit_is_zero() {
        let mut queue = PriorityQueue::bounded(Some(0));

        assert_eq!(queue.enqueue(Priority::Zero, || {}), Err(Error::QueueFull));
    }

    #[test]
    fn it_will_enqueue_when_limit_is_one() {
        let mut queue = PriorityQueue::bounded(Some(1));

        queue
            .enqueue(Priority::Five, || {})
            .expect("It will queue an item");
    }

    #[test]
    fn it_will_dequeue_zero_priority() {
        let mut queue = PriorityQueue::bounded(Some(1));

        let (tx, rx) = channel();

        let task = move || {
            tx.send(Priority::Zero as u8).expect("it sends the value");
        };

        queue
            .enqueue(Priority::Zero, task)
            .expect("It will queue an item");

        // Run our task to update the found task priority
        // If we dequeued the correct task, our priority will be set to Priority::Zero.
        queue.dequeue().expect("we dequeued a task")();

        assert_eq!(
            rx.recv().expect("receive a task response"),
            Priority::Zero as u8
        );
    }

    #[test]
    fn it_will_dequeue_seven_priority() {
        let mut queue = PriorityQueue::bounded(Some(1));

        let (tx, rx) = channel();

        let task = move || {
            tx.send(Priority::Seven as u8).expect("it sends the value");
        };

        queue
            .enqueue(Priority::Seven, task)
            .expect("It will queue an item");

        // Run our task to update the found task priority
        // If we dequeued the correct task, our priority will be set to Priority::Seven.
        queue.dequeue().expect("we dequeued a task")();

        assert_eq!(
            rx.recv().expect("receive a task response"),
            Priority::Seven as u8
        );
    }

    #[test]
    fn it_will_dequeue_highest_priority() {
        let mut queue = PriorityQueue::bounded(Some(2));

        let (tx, rx) = channel();

        let my_tx = tx.clone();

        let task = move || {
            my_tx
                .send(Priority::Five as u8)
                .expect("it sends the value");
        };

        queue
            .enqueue(Priority::Five, task)
            .expect("It will queue an item");

        let task = move || {
            tx.send(Priority::Four as u8).expect("it sends the value");
        };

        queue
            .enqueue(Priority::Four, task)
            .expect("It will queue an item");

        // Run our task to update the found task priority
        // If we dequeued the correct task, our priority will be set to Priority::Four.
        queue.dequeue().expect("we dequeued a task")();

        assert_eq!(
            rx.recv().expect("receive a task response"),
            Priority::Four as u8
        );
    }

    #[test]
    fn it_will_dequeue_random_tasks_in_the_correct_sequence() {
        let mut queue = PriorityQueue::bounded(Some(32));

        let (tx, rx) = channel();

        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, 8);

        let mut expected_order = vec![];

        for idx in 0..32 {
            let my_tx = tx.clone();
            let priority_val = rng.sample(range) as u8;
            let priority = Priority::try_from(priority_val).expect("it's a valid priority");
            let task = move || {
                my_tx.send(idx).expect("it sends the value");
            };
            queue
                .enqueue(priority, task)
                .expect("we can enqueue our task");
            expected_order.push((priority_val, idx));
        }

        assert_eq!(queue.len(), 32);

        expected_order.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        for (_, val) in expected_order {
            let task = queue.dequeue().expect("we have 32 tasks");
            // Run our task
            task();
            assert_eq!(rx.recv().expect("receive a task response"), val);
        }
    }

    #[test]
    fn it_will_drain_random_tasks_in_the_correct_sequence() {
        let mut queue = PriorityQueue::bounded(Some(32));

        let (tx, rx) = channel();

        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, 8);

        let mut expected_order = vec![];

        for idx in 0..32 {
            let my_tx = tx.clone();
            let priority_val = rng.sample(range) as u8;
            let priority = Priority::try_from(priority_val).expect("it's a valid priority");
            let task = move || {
                my_tx.send(idx).expect("it sends the value");
            };
            queue
                .enqueue(priority, task)
                .expect("we can enqueue our task");
            expected_order.push((priority_val, idx));
        }

        assert_eq!(queue.len(), 32);

        expected_order.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut expected = expected_order.into_iter();
        for task in queue.drain() {
            // Run our task
            task();
            let (_, val) = expected.next().expect("We have a task");
            assert_eq!(rx.recv().expect("receive a task response"), val);
        }
    }

    #[test]
    fn it_will_not_convert_invalid_priority() {
        let out_of_range: u8 = 8;

        assert_eq!(
            Priority::try_from(out_of_range),
            Err(Error::InvalidPriority(8))
        );
    }

    #[test]
    fn it_will_convert_valid_priority() {
        let in_range: u8 = 4;

        let priority = Priority::try_from(in_range).expect("it converts in range priorities");
        assert_eq!(priority, Priority::Four);
    }

    #[test]
    fn it_knows_when_a_queue_is_empty() {
        let mut queue = PriorityQueue::bounded(Some(1));

        assert!(queue.is_empty());
        queue
            .enqueue(Priority::Five, || {})
            .expect("It will queue an item");
        assert!(!queue.is_empty());
    }

    #[test]
    fn it_will_deliver_a_callback() {
        let queue = Arc::new(Mutex::new(PriorityQueue::bounded(Some(1))));

        let my_queue = queue.clone();

        let (tx, rx) = channel();

        std::thread::spawn(move || {
            let (tx_cb, rx_cb) = channel();
            let mut guard = my_queue.lock().unwrap();

            let mut task = guard.dequeue();

            if task.is_none() {
                guard.register(tx_cb);
                drop(guard);
                task = Some(rx_cb.recv().expect("to get a task at some point"));
            }
            tx.send(task).expect("we sent our task");
        });

        // Slight pause to allow other thread to register
        sleep(Duration::from_millis(20));

        let task = move || {};
        queue
            .lock()
            .unwrap()
            .enqueue(Priority::Zero, task)
            .expect("we can enqueue our task");
        let task = rx.recv().expect("to get a task at some point");

        assert!(task.is_some())
    }
}
