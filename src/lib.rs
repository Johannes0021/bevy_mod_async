use bevy_app::{FixedMainScheduleOrder, MainScheduleOrder, prelude::*};
use bevy_ecs::{
    prelude::*,
    schedule::{ScheduleLabel, SystemSet},
};
use bevy_log::prelude::*;
use bevy_tasks::{Task, prelude::*};
use bevy_time::prelude::*;
use futures::channel::{mpsc, oneshot};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

pub mod event;
pub mod message;

pub mod prelude {
    pub use crate::{
        AsyncContext, AsyncTaskContext, AsyncTaskPlugin, AsyncTaskSystems, RunAfter,
        SpawnTaskDeferredExt, SpawnTaskExt,
        event::{EntityEventFutureExt, EventStreamTaskExt},
        message::MessageStreamTaskExt,
    };
}

//==================================================================================================
// AsyncTaskPlugin
//==================================================================================================

pub struct AsyncTaskPlugin;

#[derive(SystemSet, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskSystems;

#[derive(ScheduleLabel, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskBeforeFixedUpdate;

#[derive(ScheduleLabel, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskBeforeUpdate;

impl Plugin for AsyncTaskPlugin {
    fn build(&self, app: &mut App) {
        app.init_schedule(AsyncTaskBeforeFixedUpdate);
        let mut fixed_main_schedule_order =
            app.world_mut().resource_mut::<FixedMainScheduleOrder>();
        fixed_main_schedule_order.insert_before(FixedUpdate, AsyncTaskBeforeFixedUpdate);

        app.init_schedule(AsyncTaskBeforeUpdate);
        let mut main_schedule_order = app.world_mut().resource_mut::<MainScheduleOrder>();
        main_schedule_order.insert_before(Update, AsyncTaskBeforeUpdate);

        app.insert_resource(AsyncContext::new())
            .add_systems(
                AsyncTaskBeforeFixedUpdate,
                fixed_update_and_queue_scheduled_world_tasks.in_set(AsyncTaskSystems),
            )
            .add_systems(
                AsyncTaskBeforeUpdate,
                (
                    update_and_queue_scheduled_world_tasks,
                    run_async_world_tasks,
                )
                    .chain()
                    .in_set(AsyncTaskSystems),
            )
            .add_systems(Last, receive_scheduled_world_tasks.in_set(AsyncTaskSystems));
    }
}

pub fn run_async_world_tasks(world: &mut World, mut world_tasks: Local<Vec<WorldTask>>) {
    loop {
        let mut cx = world.resource_mut::<AsyncContext>();
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
    world_task_tx: &mpsc::UnboundedSender<WorldTask>,
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
            mpsc_unbounded_send_with_error_api_guard(
                world_task_tx,
                scheduled_task.task,
                Some(
                    "Failed to send task to `run_async_world_tasks`. \
                        Did you remove `AsyncContext` resource?"
                        .to_string(),
                ),
            );
        } else {
            i += 1;
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
                Delay::Elapsed(checked_duration_from_secs_f64(secs)),
            ),
            RunAfter::FixedUpdateElapsedSecs(secs) => (
                &mut cx.scheduled_fixed_update_tasks,
                Delay::Elapsed(checked_duration_from_secs_f64(secs)),
            ),
        };

        queue.push_back(ScheduledWorldTask {
            delay,
            task: scheduled_task.task,
        });
    }
}

//==================================================================================================
// WorldTask
//==================================================================================================

type WorldTask = Box<dyn FnOnce(&mut World) + Send + Sync>;

//==================================================================================================
// WorldTaskResultReceiver
//==================================================================================================

#[must_use]
pub struct WorldTaskResultReceiver<R>(oneshot::Receiver<R>);

impl<R> Future for WorldTaskResultReceiver<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let pinned_result_rx = Pin::new(&mut this.0);
        match pinned_result_rx.poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // Sender was dropped, most likely during app shutdown.
            // Ignore the disconnect and keep the future pending.
            Poll::Ready(Err(_)) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> WorldTaskResultReceiver<R>
where
    R: Send + 'static,
{
    fn new<F>(world_task_tx: &mpsc::UnboundedSender<WorldTask>, f: F) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();

        // If this fails the receiver was dropped, most likely during app shutdown.
        // Ignore the disconnect and return a future that never resolves.
        mpsc_unbounded_send_with_error_api_guard(
            world_task_tx,
            Box::new(move |world| {
                oneshot_channel_send_with_error_api_guard(result_tx, f(world), None)
            }),
            None,
        );

        Self(result_rx)
    }

    fn new_scheduled<F>(
        delay: RunAfter,
        world_task_tx: &mpsc::UnboundedSender<ScheduledWorldTask<RunAfter>>,
        f: F,
    ) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();

        // If this fails the receiver was dropped, most likely during app shutdown.
        // Ignore the disconnect and return a future that never resolves.
        mpsc_unbounded_send_with_error_api_guard(
            world_task_tx,
            ScheduledWorldTask {
                delay,
                task: Box::new(move |world| {
                    oneshot_channel_send_with_error_api_guard(result_tx, f(world), None)
                }),
            },
            None,
        );

        Self(result_rx)
    }

    pub fn detach(self) {}
}

//==================================================================================================
// ScheduledWorldTask
//==================================================================================================

struct ScheduledWorldTask<T> {
    delay: T,
    task: WorldTask,
}

//==================================================================================================
// RunAfter
//==================================================================================================

pub enum RunAfter {
    UpdateTicks(usize),
    FixedUpdateTicks(usize),
    UpdateElapsed(Duration),
    FixedUpdateElapsed(Duration),
    UpdateElapsedSecs(f64),
    FixedUpdateElapsedSecs(f64),
}

//==================================================================================================
// Delay
//==================================================================================================

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
    scheduled_update_tasks: VecDeque<ScheduledWorldTask<Delay>>,
    scheduled_fixed_update_tasks: VecDeque<ScheduledWorldTask<Delay>>,
    world_task_tx: mpsc::UnboundedSender<WorldTask>,
    world_task_rx: mpsc::UnboundedReceiver<WorldTask>,
    scheduled_world_task_tx: mpsc::UnboundedSender<ScheduledWorldTask<RunAfter>>,
    scheduled_world_task_rx: mpsc::UnboundedReceiver<ScheduledWorldTask<RunAfter>>,
}

impl Drop for AsyncContext {
    fn drop(&mut self) {
        info!("AsyncContext dropped. Existing async operations may leak resources.");
    }
}

impl AsyncContext {
    fn new() -> Self {
        let (world_task_tx, world_task_rx) = mpsc::unbounded();
        let (scheduled_world_task_tx, scheduled_world_task_rx) = mpsc::unbounded();

        Self {
            scheduled_update_tasks: VecDeque::default(),
            scheduled_fixed_update_tasks: VecDeque::default(),
            world_task_tx,
            world_task_rx,
            scheduled_world_task_tx,
            scheduled_world_task_rx,
        }
    }

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
    world_task_tx: mpsc::UnboundedSender<WorldTask>,
    scheduled_world_task_tx: mpsc::UnboundedSender<ScheduledWorldTask<RunAfter>>,
}

impl AsyncTaskContext {
    pub fn with_world<F, R>(&self, f: F) -> WorldTaskResultReceiver<R>
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        WorldTaskResultReceiver::new(&self.world_task_tx, f)
    }

    pub fn with_world_scheduled<F, R>(&self, delay: RunAfter, f: F) -> WorldTaskResultReceiver<R>
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
            WorldTaskResultReceiver::new_scheduled(delay, &self.scheduled_world_task_tx, f)
        }
    }

    pub fn delay(&self, delay: RunAfter) -> WorldTaskResultReceiver<()> {
        self.with_world_scheduled(delay, |_| {})
    }
}

//==================================================================================================
// helper functions
//==================================================================================================

/// Compile-time guard for the `oneshot::Sender<T>::send` result.
/// More robust than `let _ = tx.send(...)`.
pub(crate) fn oneshot_channel_send_with_error_api_guard<T>(
    tx: oneshot::Sender<T>,
    value: T,
    error_message: Option<String>,
) {
    match tx.send(value) {
        Ok(()) => (),
        Err(t) => {
            let _: &T = &t;

            if let Some(error_message) = error_message {
                error!("{error_message}");
            }
        }
    }
}

/// Compile-time guard for the `oneshot::Sender<T>::send` result.
/// More robust than `let _ = tx.send(...)`.
pub(crate) fn mpsc_unbounded_send_with_error_api_guard<T>(
    tx: &mpsc::UnboundedSender<T>,
    value: T,
    error_message: Option<String>,
) {
    match tx.unbounded_send(value) {
        Ok(()) => (),
        Err(t) => {
            let _: T = t.into_inner();

            if let Some(error_message) = error_message {
                error!("{error_message}");
            }
        }
    }
}

fn checked_duration_from_secs_f64(secs: f64) -> Duration {
    debug_assert!(secs >= 0.0, "Duration must be non-negative");
    Duration::from_secs_f64(secs.max(0.0))
}
