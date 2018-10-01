use crossbeam_channel as channel;
use published_value;
use std::sync::Arc;
use std::thread;
use thread_util::JoinOnDrop;
use Reducer;
use Worker;

#[derive(Debug)]
pub struct DynamicPool<I, W, R>
where
    W: Worker<I>,
    R: Reducer<W::Output>,
{
    work_sender: channel::Sender<I>,
    coordinator_thread: JoinOnDrop<()>,
    change_concurrency_limit_sender: channel::Sender<i64>,
    output_waiter: published_value::Waiter<R::Output>,
}

impl<I, W, R> DynamicPool<I, W, R>
where
    I: Send + 'static,
    W: Worker<I> + Send + Sync + 'static,
    W::Output: Send,
    R: Reducer<W::Output> + Send + 'static,
    R::Output: Send,
{
    pub(super) fn create(worker: W, reducer: R, concurrency_limit: i64) -> Self {
        let (work_sender, work_receiver) = channel::unbounded();
        let (change_concurrency_limit_sender, change_concurrency_limit_receiver) =
            channel::unbounded();
        let (output_publisher, output_waiter) = published_value::new();
        let coordinator_thread = JoinOnDrop::wrap(thread::spawn(move || {
            coordinator(
                worker,
                reducer,
                work_receiver,
                concurrency_limit,
                change_concurrency_limit_receiver,
                output_publisher,
            )
        }));
        DynamicPool {
            work_sender,
            coordinator_thread,
            output_waiter,
            change_concurrency_limit_sender,
        }
    }

    pub fn set_concurrency_limit(&self, concurrency_limit: i64) {
        self.change_concurrency_limit_sender.send(concurrency_limit);
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
}

impl<I, W, R> ::Pool<I> for DynamicPool<I, W, R>
where
    W: Worker<I>,
    R: Reducer<W::Output>,
{
    type Output = R::Output;
    type WaitHandle = WaitHandle<R::Output>;

    fn add(&self, input: I) {
        self.work_sender.send(input);
    }

    fn wait_handle(self) -> Self::WaitHandle {
        WaitHandle {
            coordinator_thread: Arc::new(self.coordinator_thread),
            change_concurrency_limit_sender: self.change_concurrency_limit_sender,
            output_waiter: self.output_waiter,
        }
    }

    fn wait(self) -> R::Output {
        drop(self.work_sender);
        let _ = self.coordinator_thread.join();
        let output_waiter = self.output_waiter;
        output_waiter.wait_for_value();
        output_waiter
            .into_value()
            .unwrap_or_else(|_| panic!("unable to get ownership of output value"))
    }
}

#[derive(Clone, Debug)]
pub struct WaitHandle<O> {
    coordinator_thread: Arc<JoinOnDrop<()>>,
    change_concurrency_limit_sender: channel::Sender<i64>,
    output_waiter: published_value::Waiter<O>,
}

impl<O> ::WaitHandle for WaitHandle<O> {
    type Output = O;

    fn wait(&self) -> &Self::Output {
        self.output_waiter.wait_for_value()
    }
}

impl<O> WaitHandle<O> {
    pub fn set_concurrency_limit(&self, concurrency_limit: i64) {
        self.change_concurrency_limit_sender.send(concurrency_limit);
    }

    pub fn wait(&self) -> &O {
        ::WaitHandle::wait(self)
    }
}

fn coordinator<I, W, R>(
    worker: W,
    reducer: R,
    work_receiver: channel::Receiver<I>,
    concurrency_limit: i64,
    change_concurrency_limit_receiver: channel::Receiver<i64>,
    output_publisher: published_value::Publisher<R::Output>,
) where
    I: Send + 'static,
    W: Worker<I> + Send + Sync + 'static,
    W::Output: Send,
    R: Reducer<W::Output> + Send + 'static,
    R::Output: Send,
{
    output_publisher.publish(coordinator_loop(
        worker,
        reducer,
        work_receiver,
        concurrency_limit,
        change_concurrency_limit_receiver,
    ));
}

fn coordinator_loop<I, W, R>(
    worker: W,
    mut reducer: R,
    work_receiver: channel::Receiver<I>,
    mut concurrency_limit: i64,
    change_concurrency_limit_receiver: channel::Receiver<i64>,
) -> R::Output
where
    I: Send + 'static,
    W: Worker<I> + Send + Sync + 'static,
    W::Output: Send,
    R: Reducer<W::Output> + Send + 'static,
    R::Output: Send,
{
    let worker = Arc::new(worker);
    let (work_input_sender, work_input_receiver) = channel::bounded(0);
    let (work_output_sender, work_output_receiver) = channel::bounded(0);
    let (kill_thread_req_sender, kill_thread_req_receiver) = channel::unbounded();
    let (kill_thread_ack_sender, kill_thread_ack_receiver) = channel::unbounded();
    let mut thread_creator = Some(Box::new(move || {
        let worker = worker.clone();
        let work_input_receiver = work_input_receiver.clone();
        let work_output_sender = work_output_sender.clone();
        let kill_thread_req_receiver = kill_thread_req_receiver.clone();
        let kill_thread_ack_sender = kill_thread_ack_sender.clone();
        JoinOnDrop::wrap(thread::spawn(move || {
            worker_loop(
                worker,
                work_input_receiver,
                work_output_sender,
                kill_thread_req_receiver,
                kill_thread_ack_sender,
            )
        }))
    }));
    let mut work_input_sender = Some(work_input_sender);
    let mut threads: Vec<JoinOnDrop<()>> = Vec::new();
    let mut num_active_threads: i64 = 0;
    let mut last_received_work: Option<Option<I>> = None;

    loop {
        last_received_work = match last_received_work {
            None => {
                select! {
                    recv(change_concurrency_limit_receiver, new_limit) => {
                        concurrency_limit = new_limit.expect("change_concurrency_limit closed unexpectedly");
                        for _ in concurrency_limit .. threads.len() as i64 {
                            kill_thread_req_sender.send(());
                        }
                        None
                    },
                    recv(kill_thread_ack_receiver, thread_id) => {
                        if let Some(thread_id) = thread_id {
                            let idx = threads.iter().position(|ref handle| handle.thread().id() == thread_id).expect("thread not found in active threads");
                            threads.swap_remove(idx);
                        }
                        None
                    },
                    recv(work_output_receiver, output) => {
                        num_active_threads -= 1;
                        let output = output.expect("work_output_receiver closed while work remaining");
                        reducer.reduce(output);
                        None
                    },
                    recv(work_receiver, work) => {
                        Some(work)
                    },
                }
            }
            Some(Some(work)) => {
                let idle_threads = threads.len() as i64 - num_active_threads;
                assert!(idle_threads >= 0);
                if idle_threads == 0 && (threads.len() as i64) < concurrency_limit {
                    threads.push(thread_creator
                        .as_ref()
                        .expect("thread creator destroyed while work remains")(
                    ));
                }
                select! {
                    recv(change_concurrency_limit_receiver, new_limit) => {
                        concurrency_limit = new_limit.expect("change_concurrency_limit closed unexpectedly");
                        for _ in concurrency_limit .. threads.len() as i64 {
                            kill_thread_req_sender.send(());
                        }
                        Some(Some(work))
                    },
                    recv(kill_thread_ack_receiver, thread_id) => {
                        if let Some(thread_id) = thread_id {
                            let idx = threads.iter().position(|ref handle| handle.thread().id() == thread_id).expect("thread not found in active threads");
                            let handle = threads.swap_remove(idx);
                            let _ = handle.join();
                        }
                        Some(Some(work))
                    },
                    recv(work_output_receiver, output) => {
                        num_active_threads -= 1;
                        let output = output.expect("work_output_receiver closed while work remaining");
                        reducer.reduce(output);
                        Some(Some(work))
                    },
                    send(work_input_sender.as_ref().expect("work_input_sender dropped while work remaining"), work) => {
                        num_active_threads += 1;
                        None
                    },
                }
            }
            Some(None) => {
                work_input_sender = None;
                thread_creator = None;
                select! {
                    recv(change_concurrency_limit_receiver, new_limit) => {
                        concurrency_limit = new_limit.expect("change_concurrency_limit closed unexpectedly");
                        for _ in concurrency_limit .. threads.len() as i64 {
                            kill_thread_req_sender.send(());
                        }
                    },
                    recv(kill_thread_ack_receiver, thread_id) => {
                        if let Some(thread_id) = thread_id {
                            let idx = threads.iter().position(|ref handle| handle.thread().id() == thread_id).expect("thread not found in active threads");
                            threads.swap_remove(idx);
                        }
                    },
                    recv(work_output_receiver, output) => {
                        match output {
                            Some(output) => {
                                num_active_threads -= 1;
                                reducer.reduce(output)
                            },
                            None => {
                                for thread in threads {
                                    let _ = thread.join();
                                }
                                return reducer.output();
                            },
                        };
                    },
                };
                Some(None)
            }
        }
    }
}

fn worker_loop<W, I>(
    worker: Arc<W>,
    work_receiver: channel::Receiver<I>,
    work_sender: channel::Sender<W::Output>,
    kill_req: channel::Receiver<()>,
    kill_ack: channel::Sender<thread::ThreadId>,
) where
    W: Worker<I>,
{
    loop {
        if kill_req.try_recv().is_some() {
            kill_ack.send(thread::current().id());
            return;
        }
        select! {
            recv(kill_req, _) => {
                kill_ack.send(thread::current().id());
                return;
            },
            recv(work_receiver, work_item) => match work_item {
                Some(work) => {
                    let work_output = worker.run(work);
                    work_sender.send(work_output);
                },
                None => {
                    return
                },
            },
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
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
            .set_concurrency_limit(0)
            .create_dynamic_pool();
        pool.add(2);
        pool.add(4);
        pool.set_concurrency_limit(100);
        assert_eq!(pool.wait(), 2 * 2 + 4 * 2);
    }

    #[test]
    fn wait_handle() {
        let pool = new()
            .set_worker(worker)
            .set_reducer(SumReducer(0))
            .set_concurrency_limit(0)
            .create_dynamic_pool();
        pool.add(2);
        pool.add(4);
        let wait_handle = pool.wait_handle();
        wait_handle.set_concurrency_limit(100);
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
                .set_concurrency_limit(100)
                .create_dynamic_pool();
            pool.add(2);
            pool.add(4);
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
    fn forget_to_wait_on_wait_handle() {
        let reducer: Arc<Mutex<CountCalls>> = Arc::new(Mutex::new(Default::default()));
        {
            let pool = new()
                .set_reducer(reducer.clone())
                .set_worker(worker)
                .set_concurrency_limit(100)
                .create_dynamic_pool();
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
            .set_concurrency_limit(100)
            .set_worker(|i: i64| -> i64 { i * 100 })
            .collect_into::<Vec<_>>()
            .create_dynamic_pool();
        pool.add(2);
        pool.add(3);
        pool.add(10);
        assert_eq!(pool.wait(), vec![200, 300, 1000]);
    }
}
