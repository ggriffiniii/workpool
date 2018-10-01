use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub struct JoinOnDrop<T>(Option<JoinHandle<T>>);

impl<T> JoinOnDrop<T> {
    pub fn wrap(handle: JoinHandle<T>) -> JoinOnDrop<T> {
        JoinOnDrop(Some(handle))
    }

    pub fn join(mut self) -> thread::Result<T> {
        self.0.take().unwrap().join()
    }

    pub fn thread(&self) -> &thread::Thread {
        self.0.as_ref().unwrap().thread()
    }
}

impl<T> Drop for JoinOnDrop<T> {
    fn drop(&mut self) {
        if let Some(join_handle) = self.0.take() {
            let _ = join_handle.join();
        }
    }
}
