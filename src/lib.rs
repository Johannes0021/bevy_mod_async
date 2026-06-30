use bevy_app::{App, Last, Plugin};
use bevy_ecs::{
    resource::Resource,
    system::{Commands, Local},
    world::World,
};
use bevy_tasks::{AsyncComputeTaskPool, Task};
use futures::task::AtomicWaker;
use std::{
    future::Future,
    marker::Send,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub mod event;
pub mod message;

pub mod prelude {
    pub use crate::{
        AsyncContext, AsyncTaskContext, AsyncTaskPlugin, SpawnTaskDeferredExt, SpawnTaskExt,
        event::{EntityEventFutureExt, EventStreamTaskExt},
        message::MessageStreamTaskExt,
    };
}

//==================================================================================================
// AsyncTaskPlugin
//==================================================================================================

/// Adds [`AsyncContext`] resource to world to handle async world tasks spawned from
/// [`AsyncTaskContext::with_world`], and schedules [`run_async_world_tasks`] in [`Last`] to
/// dispatch them.
pub struct AsyncTaskPlugin;

impl Plugin for AsyncTaskPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<AsyncContext>();
        app.add_systems(Last, run_async_world_tasks);
    }
}

/// This system dispatches async world tasks that need exclusive [`World`] access
/// (any tasks created with [`AsyncTaskContext::with_world`]). This system can be moved around to
/// control how often and when these tasks are dispatched.
pub fn run_async_world_tasks(world: &mut World, mut world_tasks: Local<Vec<WorldTask>>) {
    loop {
        let work = world.resource_mut::<AsyncContext>();
        while let Ok(task) = work.world_task_rx.try_recv() {
            world_tasks.push(task);
        }

        if world_tasks.is_empty() {
            break;
        }

        for task in world_tasks.drain(..) {
            task(world);
        }
    }
}

//==================================================================================================
// WorldTask
//==================================================================================================

type WorldTask = Box<dyn FnOnce(&mut World) + Send>;

//==================================================================================================
// SpawnTaskExt
//==================================================================================================

pub trait SpawnTaskExt {
    /// Spawn a task onto Bevy's async executor. The [`AsyncComputeTaskPool`] must have been
    /// initialized before this method is called (this is done automatically by [`TaskPoolPlugin`]).
    fn spawn_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static;
}

impl SpawnTaskExt for World {
    fn spawn_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let cx = self.resource::<AsyncContext>().create_task_context();
        AsyncComputeTaskPool::get().spawn(task(cx))
    }
}

impl SpawnTaskExt for AsyncContext {
    fn spawn_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let cx = self.create_task_context();
        AsyncComputeTaskPool::get().spawn(task(cx))
    }
}

impl SpawnTaskExt for AsyncTaskContext {
    fn spawn_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let this = self.clone();
        AsyncComputeTaskPool::get().spawn(task(this))
    }
}

//==================================================================================================
// SpawnTaskDeferredExt
//==================================================================================================

pub trait SpawnTaskDeferredExt {
    /// Spawn a task onto Bevy's async executor. The [`AsyncComputeTaskPool`] must be have been
    /// initialized before this command is applied (this is done automatically by
    /// [`TaskPoolPlugin`]).
    fn spawn_task<T, F>(&mut self, task: T)
    where
        T: FnOnce(AsyncTaskContext) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static;
}

impl SpawnTaskDeferredExt for Commands<'_, '_> {
    fn spawn_task<T, F>(&mut self, task: T)
    where
        T: FnOnce(AsyncTaskContext) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.queue(move |world: &mut World| {
            world.spawn_task(task).detach();
        });
    }
}

//==================================================================================================
// AsyncContext
//==================================================================================================

/// This resource owns a queue for work that needs exclusive [`World`] access. Calling
/// [`create_task_context`] will give you a [`AsyncTaskContext`] that can be used to schedule
/// work onto the queue.
#[derive(Resource)]
pub struct AsyncContext {
    world_task_tx: crossbeam_channel::Sender<WorldTask>,
    world_task_rx: crossbeam_channel::Receiver<WorldTask>,
}

impl Default for AsyncContext {
    fn default() -> Self {
        let (world_task_tx, world_task_rx) = crossbeam_channel::unbounded();
        Self {
            world_task_tx,
            world_task_rx,
        }
    }
}

impl AsyncContext {
    /// Create a [`AsyncTaskContext`] which can schedule work onto this struct's queue.
    /// This work will be run next time [`run_async_world_tasks`] runs,
    /// which by default happens once per frame in [`Last`].
    pub fn create_task_context(&self) -> AsyncTaskContext {
        AsyncTaskContext {
            world_task_tx: self.world_task_tx.clone(),
        }
    }
}

//==================================================================================================
// AsyncTaskContext
//==================================================================================================

/// This is an adapter between async tasks and [`AsyncContext`].
/// This struct gets passed as a paramter into all new async tasks and can be used to send work to
/// get run with exclusive world access.
/// You can create one with [`AsyncContext::create_task_context`],
/// or this will be done for you when you spawn a task with [`commands.spawn_task()`].
#[derive(Clone)]
pub struct AsyncTaskContext {
    world_task_tx: crossbeam_channel::Sender<WorldTask>,
}

impl AsyncTaskContext {
    /// Execute a task with mutable world access.
    /// The task `f` is scheduled to be run the next time [`run_async_world_tasks`] is run,
    /// which by default happens once per frame in the [`Last`] schedule.
    /// For this reason, small tasks should be batched so they aren't scheduled with a frame delay
    /// between them.
    pub fn with_world<F, R>(&self, f: F) -> WithWorldFuture<R>
    where
        F: FnOnce(&mut World) -> R + Send + 'static,
        R: Send + 'static,
    {
        WithWorldFuture::new(f, &self.world_task_tx)
    }
}

//==================================================================================================
// WithWorldFuture
//==================================================================================================

#[must_use = "future must be awaited to yield execution or detached"]
pub struct WithWorldFuture<R> {
    waker_tx: Arc<AtomicWaker>,
    result_rx: crossbeam_channel::Receiver<R>,
}

impl<R> Future for WithWorldFuture<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.waker_tx.register(cx.waker());

        match self.result_rx.try_recv() {
            Ok(v) => Poll::Ready(v),
            Err(crossbeam_channel::TryRecvError::Empty) => Poll::Pending,
            Err(crossbeam_channel::TryRecvError::Disconnected) => panic!("channel closed"),
        }
    }
}

impl<R> WithWorldFuture<R>
where
    R: Send + 'static,
{
    fn new<F>(f: F, work_queue: &crossbeam_channel::Sender<WorldTask>) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + 'static,
    {
        let waker_tx = Arc::new(AtomicWaker::new());
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);

        let waker_rx = waker_tx.clone();
        work_queue
            .send(Box::new(move |world| {
                // If this `send` fails, most likely the user forgot to `await` this future,
                // and they should have a warning anyway, so we're going to completely ignore this.
                send_with_error_api_guard(&result_tx, f(world));
                waker_rx.wake();
            }))
            .expect(
                "Failed to send task to `run_async_world_tasks`. Did you remove `AsyncContext` resource?",
            );

        Self {
            waker_tx,
            result_rx,
        }
    }

    /// Discard the return value of this task and allow it to finish concurrently within the
    /// executor. This is useful for when your task does not return a value, e.g. when it simply
    /// mutates the world. This allows you to queue many tasks using `with_world` so they can
    /// potentially be dispatched within the same frame.
    pub fn detach(self) {}
}

//==================================================================================================
// helper functions
//==================================================================================================

/// Compile-time structural guard for `crossbeam_channel::SendError<T>`.
///
/// This function forces the compiler to depend on the concrete structure of `SendError<T>` so that
/// any breaking change in the dependency will surface as a compilation error.
///
/// It is not a runtime error-handling mechanism and does not guarantee exhaustive handling of all
/// future error conditions.
///
/// More robust than `let _ = tx.send(...)`.
pub(crate) fn send_with_error_api_guard<T>(tx: &crossbeam_channel::Sender<T>, value: T) {
    let result = tx.send(value);

    if let Err(crossbeam_channel::SendError(t)) = result {
        let _ = &t;
    }
}
