use crossbeam_channel as channel;
use published_value;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::thread;
use thread_util::JoinOnDrop;
use Reducer;
use Worker;

#[derive(Debug)]
pub struct StaticPool<I, W, R>
where
    W: Worker<I>,
    R: Reducer<W::Output>,
{
    worker: PhantomData<W>,
    work_sender: channel::Sender<I>,
    reducer: Arc<Mutex<R>>,
    threads: Vec<JoinOnDrop<()>>,
}

impl<I, W, R> StaticPool<I, W, R>
where
    I: Send + 'static,
    W: Worker<I> + Send + Sync + 'static,
    W::Output: Send,
    R: Reducer<W::Output> + Send + 'static,
    R::Output: Send,
{
    pub(super) fn create(worker: W, reducer: R, concurrency_limit: i64) -> Self {
        let worker = Arc::new(worker);
        let reducer = Arc::new(Mutex::new(reducer));
        let (work_sender, work_receiver) = channel::unbounded();
        let threads: Vec<_> = (0..concurrency_limit)
            .map(|_| {
                Self::start_worker_thread(worker.clone(), reducer.clone(), work_receiver.clone())
            }).map(JoinOnDrop::wrap)
            .collect();
        StaticPool {
            worker: PhantomData {},
            work_sender,
            reducer,
            threads,
        }
    }

    pub fn add(&self, input: I) {
        ::Pool::<I>::add(self, input)
    }

    pub fn wait_handle(self) -> WaitHandle<R::Output> {
        ::Pool::<I>::wait_handle(self)
    }

    pub fn wait(self) -> R::Output {
        ::Pool::<I>::wait(self)
    }

    fn start_worker_thread(
        worker: Arc<W>,
        reducer: Arc<Mutex<R>>,
        work_receiver: channel::Receiver<I>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            for work in work_receiver {
                let output = worker.run(work);
                reducer.lock().expect("lock poisoned").reduce(output);
            }
        })
    }
}

impl<I, W, R> ::Pool<I> for StaticPool<I, W, R>
where
    I: Send + 'static,
    W: Worker<I> + Send + Sync + 'static,
    R: Reducer<W::Output> + Send + 'static,
    R::Output: Send,
{
    type Output = R::Output;
    type WaitHandle = WaitHandle<R::Output>;

    fn add(&self, input: I) {
        self.work_sender.send(input);
    }

    fn wait_handle(self) -> Self::WaitHandle {
        drop(self.work_sender);
        let (output_publisher, output_waiter) = published_value::new();
        let threads = self.threads;
        let reducer = self.reducer;
        let wait_thread = Arc::new(JoinOnDrop::wrap(thread::spawn(move || {
            drop(threads);
            let reducer = Arc::try_unwrap(reducer)
                .unwrap_or_else(|_| panic!("unable to acquire ownership of reducer"))
                .into_inner()
                .expect("lock poisoned");
            output_publisher.publish(reducer.output());
        })));
        WaitHandle {
            wait_thread,
            output_waiter,
        }
    }

    fn wait(self) -> R::Output {
        drop(self.work_sender);
        drop(self.threads);
        Arc::try_unwrap(self.reducer)
            .unwrap_or_else(|_| panic!("unable to acquire ownership of reducer"))
            .into_inner()
            .expect("lock poisoned")
            .output()
    }
}

#[derive(Clone, Debug)]
pub struct WaitHandle<O> {
    wait_thread: Arc<JoinOnDrop<()>>,
    output_waiter: published_value::Waiter<O>,
}

impl<O> WaitHandle<O> {
    pub fn wait(&self) -> &O {
        ::WaitHandle::wait(self)
    }
}

impl<O> ::WaitHandle for WaitHandle<O> {
    type Output = O;

    fn wait(&self) -> &Self::Output {
        self.output_waiter.wait_for_value()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use new;

    struct SumReducer(i64);
    impl Reducer<i64> for SumReducer {
        type Output = i64;
        fn reduce(&mut self, input: i64) {
            self.0 += input;
        }
        fn output(self) -> i64 {
            self.0
        }
    }
    fn worker(input: i64) -> i64 {
        input * 2
    }

    #[test]
    fn basic() {
        let pool = new()
            .set_worker(worker)
            .set_reducer(SumReducer(0))
            .set_concurrency_limit(10)
            .create_static_pool();
        pool.add(2);
        pool.add(4);
        assert_eq!(pool.wait(), 2 * 2 + 4 * 2);
    }

    #[test]
    fn wait_handle() {
        let pool = new()
            .set_worker(worker)
            .set_reducer(SumReducer(0))
            .set_concurrency_limit(10)
            .create_static_pool();
        pool.add(2);
        pool.add(4);
        let wait_handle = pool.wait_handle();
        assert_eq!(wait_handle.wait().clone(), 2 * 2 + 4 * 2);
        assert_eq!(wait_handle.clone().wait().clone(), 2 * 2 + 4 * 2);
    }

    // A reducer that simply counts the number of reduce and output calls.
    #[derive(Debug, PartialEq, Default)]
    struct CountCalls {
        reduce_calls: i32,
        output_calls: i32,
    }
    impl<T> Reducer<T> for Arc<Mutex<CountCalls>> {
        type Output = ();
        fn reduce(&mut self, _input: T) {
            let mut lock = self.lock().expect("lock poisoned");
            lock.reduce_calls += 1;
        }
        fn output(self) -> () {
            let mut lock = self.lock().expect("lock poisoned");
            lock.output_calls += 1;
        }
    }

    #[test]
    fn forget_to_wait() {
        let reducer: Arc<Mutex<CountCalls>> = Arc::new(Mutex::new(Default::default()));
        {
            let pool = new()
                .set_worker(worker)
                .set_reducer(reducer.clone())
                .set_concurrency_limit(10)
                .create_static_pool();
            pool.add(2);
            pool.add(4);
        }
        let count_calls = Arc::try_unwrap(reducer).unwrap().into_inner().unwrap();
        assert_eq!(
            count_calls,
            CountCalls {
                reduce_calls: 2,
                output_calls: 0,
            }
        );
    }

    #[test]
    fn forget_to_wait_on_wait_handle() {
        let reducer: Arc<Mutex<CountCalls>> = Arc::new(Mutex::new(Default::default()));
        {
            let pool = new()
                .set_reducer(reducer.clone())
                .set_worker(worker)
                .set_concurrency_limit(10)
                .create_static_pool();
            pool.add(2);
            pool.add(4);
            pool.wait_handle();
        }
        let count_calls = Arc::try_unwrap(reducer).unwrap().into_inner().unwrap();
        assert_eq!(
            count_calls,
            CountCalls {
                reduce_calls: 2,
                output_calls: 1,
            }
        );
    }

    #[test]
    fn collect_into_vec() {
        let pool = new()
            .set_concurrency_limit(10)
            .set_worker(|i: i64| -> i64 { i * 100 })
            .collect_into::<Vec<_>>()
            .create_static_pool();
        pool.add(2);
        pool.add(3);
        pool.add(10);
        assert_eq!(pool.wait(), vec![200, 300, 1000]);
    }
}
