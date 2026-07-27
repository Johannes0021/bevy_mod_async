use bevy_app::{App, FixedUpdate, Last, Plugin, Update};
use bevy_ecs::{
    change_detection::{Res, ResMut},
    resource::Resource,
    schedule::IntoScheduleConfigs,
    system::{Commands, Local},
    world::World,
};
use bevy_tasks::{AsyncComputeTaskPool, Task};
use bevy_time::Time;
use futures::task::AtomicWaker;
use std::{
    collections::VecDeque,
    future::Future,
    marker::Send,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

pub mod event;
pub mod message;

pub mod prelude {
    pub use crate::{
        AsyncContext, AsyncTaskContext, AsyncTaskPlugin, RunAfter, SpawnTaskDeferredExt,
        SpawnTaskExt,
        event::{EntityEventFutureExt, EventStreamTaskExt},
        message::MessageStreamTaskExt,
    };
}

//==================================================================================================
// AsyncTaskPlugin
//==================================================================================================

pub struct AsyncTaskPlugin;

impl Plugin for AsyncTaskPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<AsyncContext>()
            .add_systems(FixedUpdate, fixed_update_and_queue_scheduled_world_tasks)
            .add_systems(
                Update,
                (
                    update_and_queue_scheduled_world_tasks,
                    run_async_world_tasks,
                )
                    .chain(),
            )
            .add_systems(Last, receive_scheduled_world_tasks);
    }
}

pub fn run_async_world_tasks(world: &mut World, mut world_tasks: Local<Vec<WorldTask>>) {
    loop {
        let cx = world.resource_mut::<AsyncContext>();
        while let Ok(task) = cx.world_task_rx.try_recv() {
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

fn receive_scheduled_world_tasks(mut cx: ResMut<AsyncContext>) {
    while let Ok(scheduled_task) = cx.scheduled_world_task_rx.try_recv() {
        let (queue, delay) = match scheduled_task.delay {
            RunAfter::UpdateTicks(ticks) => (&mut cx.scheduled_update_tasks, Delay::Ticks(ticks)),
            RunAfter::FixedUpdateTicks(ticks) => {
                (&mut cx.scheduled_fixed_update_tasks, Delay::Ticks(ticks))
            }
            RunAfter::UpdateElapsed(duration) => {
                (&mut cx.scheduled_update_tasks, Delay::Elapsed(duration))
            }
            RunAfter::FixedUpdateElapsed(duration) => (
                &mut cx.scheduled_fixed_update_tasks,
                Delay::Elapsed(duration),
            ),
            RunAfter::UpdateElapsedSecs(secs) => (
                &mut cx.scheduled_update_tasks,
                Delay::Elapsed(Duration::from_secs_f64(secs)),
            ),
            RunAfter::FixedUpdateElapsedSecs(secs) => (
                &mut cx.scheduled_fixed_update_tasks,
                Delay::Elapsed(Duration::from_secs_f64(secs)),
            ),
        };

        queue.push_back(ScheduledWorldTask {
            delay,
            task: scheduled_task.task,
        });
    }
}

fn update_and_queue_scheduled_world_tasks(mut cx: ResMut<AsyncContext>, time: Res<Time>) {
    if cx.scheduled_update_tasks.is_empty() {
        return;
    }

    let AsyncContext {
        world_task_tx,
        scheduled_update_tasks,
        ..
    } = &mut *cx;

    update_and_queue_scheduled_world_tasks_helper(
        scheduled_update_tasks,
        world_task_tx,
        time.delta(),
    );
}

fn fixed_update_and_queue_scheduled_world_tasks(mut cx: ResMut<AsyncContext>, time: Res<Time>) {
    if cx.scheduled_fixed_update_tasks.is_empty() {
        return;
    }

    let AsyncContext {
        world_task_tx,
        scheduled_fixed_update_tasks,
        ..
    } = &mut *cx;

    update_and_queue_scheduled_world_tasks_helper(
        scheduled_fixed_update_tasks,
        world_task_tx,
        time.delta(),
    );
}

fn update_and_queue_scheduled_world_tasks_helper(
    scheduled_tasks: &mut VecDeque<ScheduledWorldTask<Delay>>,
    world_task_tx: &crossbeam_channel::Sender<WorldTask>,
    dt: Duration,
) {
    let mut i = 0;
    while i < scheduled_tasks.len() {
        let scheduled_task = &mut scheduled_tasks[i];

        let ready_to_queue = match &mut scheduled_task.delay {
            Delay::Ticks(ticks) => {
                *ticks = ticks.saturating_sub(1);
                *ticks == 0
            }
            Delay::Elapsed(duration) => {
                *duration = duration.saturating_sub(dt);
                duration.is_zero()
            }
        };

        if ready_to_queue {
            let scheduled_task = scheduled_tasks.remove(i).unwrap();
            world_task_tx.send(scheduled_task.task).expect(
                "Failed to send task to `run_async_world_tasks`. \
                    Did you remove `AsyncContext` resource?",
            );
        } else {
            i += 1;
        }
    }
}

//==================================================================================================
// WorldTask
//==================================================================================================

type WorldTask = Box<dyn FnOnce(&mut World) + Send + Sync>;

//==================================================================================================
// ScheduledWorldTask
//==================================================================================================

struct ScheduledWorldTask<T> {
    delay: T,
    task: WorldTask,
}

pub enum RunAfter {
    UpdateTicks(usize),
    FixedUpdateTicks(usize),
    UpdateElapsed(Duration),
    FixedUpdateElapsed(Duration),
    UpdateElapsedSecs(f64),
    FixedUpdateElapsedSecs(f64),
}

enum Delay {
    Ticks(usize),
    Elapsed(Duration),
}

//==================================================================================================
// SpawnTaskExt
//==================================================================================================

pub trait SpawnTaskExt {
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

#[derive(Resource)]
pub struct AsyncContext {
    world_task_tx: crossbeam_channel::Sender<WorldTask>,
    world_task_rx: crossbeam_channel::Receiver<WorldTask>,
    scheduled_world_task_tx: crossbeam_channel::Sender<ScheduledWorldTask<RunAfter>>,
    scheduled_world_task_rx: crossbeam_channel::Receiver<ScheduledWorldTask<RunAfter>>,
    scheduled_update_tasks: VecDeque<ScheduledWorldTask<Delay>>,
    scheduled_fixed_update_tasks: VecDeque<ScheduledWorldTask<Delay>>,
}

impl Default for AsyncContext {
    fn default() -> Self {
        let (world_task_tx, world_task_rx) = crossbeam_channel::unbounded();
        let (scheduled_world_task_tx, scheduled_world_task_rx) = crossbeam_channel::unbounded();

        Self {
            world_task_tx,
            world_task_rx,
            scheduled_world_task_tx,
            scheduled_world_task_rx,
            scheduled_update_tasks: Default::default(),
            scheduled_fixed_update_tasks: Default::default(),
        }
    }
}

impl AsyncContext {
    pub fn create_task_context(&self) -> AsyncTaskContext {
        AsyncTaskContext {
            world_task_tx: self.world_task_tx.clone(),
            scheduled_world_task_tx: self.scheduled_world_task_tx.clone(),
        }
    }
}

//==================================================================================================
// AsyncTaskContext
//==================================================================================================

#[derive(Clone)]
pub struct AsyncTaskContext {
    world_task_tx: crossbeam_channel::Sender<WorldTask>,
    scheduled_world_task_tx: crossbeam_channel::Sender<ScheduledWorldTask<RunAfter>>,
}

impl AsyncTaskContext {
    pub fn with_world<F, R>(&self, f: F) -> WithWorldFuture<R>
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        WithWorldFuture::new(&self.world_task_tx, f)
    }

    pub fn with_world_scheduled<F, R>(&self, delay: RunAfter, f: F) -> WithWorldFuture<R>
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        let ready_to_queue = match &delay {
            RunAfter::UpdateTicks(ticks) | RunAfter::FixedUpdateTicks(ticks) => *ticks == 0,

            RunAfter::UpdateElapsed(duration) | RunAfter::FixedUpdateElapsed(duration) => {
                duration.is_zero()
            }

            RunAfter::UpdateElapsedSecs(secs) | RunAfter::FixedUpdateElapsedSecs(secs) => {
                *secs <= 0.0
            }
        };

        if ready_to_queue {
            self.with_world(f)
        } else {
            WithWorldFuture::new_scheduled(delay, &self.scheduled_world_task_tx, f)
        }
    }

    pub fn delay(&self, delay: RunAfter) -> WithWorldFuture<()> {
        self.with_world_scheduled(delay, |_| {})
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
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                panic!("Failed to receive result. Did you remove `AsyncContext` resource?",)
            }
        }
    }
}

impl<R> WithWorldFuture<R>
where
    R: Send + 'static,
{
    fn new<F>(world_task_tx: &crossbeam_channel::Sender<WorldTask>, f: F) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
    {
        let waker_tx = Arc::new(AtomicWaker::new());
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);

        let waker_rx = waker_tx.clone();
        world_task_tx
            .send(Box::new(move |world| {
                // If this `send` fails, most likely the user forgot to `await` this future,
                // and they should have a warning anyway, so we're going to completely ignore this.
                send_with_error_api_guard(&result_tx, f(world));
                waker_rx.wake();
            }))
            .expect(
                "Failed to send task to `run_async_world_tasks`. \
                Did you remove `AsyncContext` resource?",
            );

        Self {
            waker_tx,
            result_rx,
        }
    }

    fn new_scheduled<F>(
        delay: RunAfter,
        world_task_tx: &crossbeam_channel::Sender<ScheduledWorldTask<RunAfter>>,
        f: F,
    ) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
    {
        let waker_tx = Arc::new(AtomicWaker::new());
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);

        let waker_rx = waker_tx.clone();
        world_task_tx
            .send(ScheduledWorldTask {
                delay,
                task: Box::new(move |world| {
                    // If this `send` fails, most likely the user forgot to `await` this future, and
                    // they should have a warning anyway, so we're going to completely ignore this.
                    send_with_error_api_guard(&result_tx, f(world));
                    waker_rx.wake();
                }),
            })
            .expect(
                "Failed to send task to `receive_scheduled_world_tasks`. \
                Did you remove `AsyncContext` resource?",
            );

        Self {
            waker_tx,
            result_rx,
        }
    }

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
