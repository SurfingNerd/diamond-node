// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

use crate::{
    IoHandler, LOCAL_STACK_SIZE,
    service_mio::{HandlerId, IoChannel, IoContext},
};
use deque;
use futures::{
    Future,
    future::{self, FutureResult, Loop},
};
use std::{
    io::{Error, ErrorKind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    thread::{self, JoinHandle},
};
use tokio::{self};

use parking_lot::{Condvar, Mutex};

const STACK_SIZE: usize = 16 * 1024 * 1024;

pub enum WorkType<Message> {
    Readable,
    Writable,
    Hup,
    Timeout,
    Message(Arc<Message>),
}

pub struct Work<Message> {
    pub work_type: WorkType<Message>,
    pub token: usize,
    pub handler_id: HandlerId,
    pub handler: Arc<dyn IoHandler<Message>>,
}

/// An IO worker thread
/// Sorts them ready for blockchain insertion.
pub struct Worker {
    thread: Option<JoinHandle<()>>,
    wait: Arc<Condvar>,
    deleting: Arc<AtomicBool>,
    wait_mutex: Arc<Mutex<()>>,
}

struct WorkerContext<Message>
where
    Message: Send + Sync + 'static,
{
    stealer: deque::Stealer<Work<Message>>,
    channel: IoChannel<Message>,
    wait: Arc<Condvar>,
    wait_mutex: Arc<Mutex<()>>,
    deleting: Arc<AtomicBool>,
}

impl<Message> WorkerContext<Message>
where
    Message: Send + Sync + 'static,
{
    pub fn new(
        stealer: deque::Stealer<Work<Message>>,
        channel: IoChannel<Message>,
        wait: Arc<Condvar>,
        wait_mutex: Arc<Mutex<()>>,
        deleting: Arc<AtomicBool>,
    ) -> Self {
        WorkerContext {
            stealer,
            channel: channel.clone(),
            wait,
            wait_mutex: wait_mutex.clone(),
            deleting,
        }
    }

    fn execute(self) -> future::FutureResult<(Self, bool), ()> {
        {
            if self.deleting.load(AtomicOrdering::SeqCst) {
                return future::ok((self, false)); // Loop::Break(());// futures::future::err(Error::new(ErrorKind::Other, "shutting down worker")); // self; // Ok(Loop::Break(self));
                //return Ok() //Ok(Loop::Break(()));
            }
            let mut lock = self.wait_mutex.lock();
            self.wait.wait(&mut lock);
        }

        while !self.deleting.load(AtomicOrdering::SeqCst) {
            match self.stealer.steal() {
                deque::Steal::Success(work) => WorkerContext::do_work(work, self.channel.clone()),
                deque::Steal::Retry => {}
                deque::Steal::Empty => break,
            }
        }

        return future::ok((self, true)); // Loop::Continue(self);
    }

    fn do_work(work: Work<Message>, channel: IoChannel<Message>) {
        match work.work_type {
            WorkType::Readable => {
                work.handler
                    .stream_readable(&IoContext::new(channel, work.handler_id), work.token);
            }
            WorkType::Writable => {
                work.handler
                    .stream_writable(&IoContext::new(channel, work.handler_id), work.token);
            }
            WorkType::Hup => {
                work.handler
                    .stream_hup(&IoContext::new(channel, work.handler_id), work.token);
            }
            WorkType::Timeout => {
                work.handler
                    .timeout(&IoContext::new(channel, work.handler_id), work.token);
            }
            WorkType::Message(message) => {
                work.handler
                    .message(&IoContext::new(channel, work.handler_id), &*message);
            }
        }
    }
}

impl Worker {
    /// Creates a new worker instance.
    pub fn new<Message>(
        name: &str,
        stealer: deque::Stealer<Work<Message>>,
        channel: IoChannel<Message>,
        wait: Arc<Condvar>,
        wait_mutex: Arc<Mutex<()>>,
    ) -> Worker
    where
        Message: Send + Sync + 'static,
    {
        let deleting = Arc::new(AtomicBool::new(false));
        let mut worker = Worker {
            thread: None,
            wait: wait.clone(),
            deleting: deleting.clone(),
            wait_mutex: wait_mutex.clone(),
        };

        let context = WorkerContext::new(stealer, channel, wait, wait_mutex, deleting);

        worker.thread = Some(
            thread::Builder::new()
                .stack_size(STACK_SIZE)
                .name(format!("Worker {}", name))
                .spawn(move || {
                    LOCAL_STACK_SIZE.with(|val| val.set(STACK_SIZE));

                    let f = |c: WorkerContext<Message>| {
                        c.execute().and_then(|r| {
                            let (context, continue_loop) = r;
                            if continue_loop {
                                Ok(Loop::Continue(context))
                            } else {
                                Ok(Loop::Break(()))
                            }
                        })
                    };

                    future::loop_fn(context, f);
                })
                .expect("Error creating worker thread"),
        );
        worker
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        trace!(target: "shutdown", "[IoWorker] Closing...");
        let _ = self.wait_mutex.lock();
        self.deleting.store(true, AtomicOrdering::SeqCst);
        self.wait.notify_all();
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
        trace!(target: "shutdown", "[IoWorker] Closed");
    }
}
