use bevy_app::{FixedMainScheduleOrder, MainScheduleOrder, prelude::*};
use bevy_ecs::{
    prelude::*,
    schedule::{ScheduleLabel, SystemSet},
};
use bevy_log::prelude::*;
use bevy_tasks::{Task, prelude::*};
use bevy_time::prelude::*;
use futures::{
    channel::{mpsc, oneshot},
    future::BoxFuture,
    prelude::*,
};
use std::{
    mem,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

pub mod event;
pub mod message;

pub mod prelude {
    pub use crate::{
        AsyncContext, AsyncTaskBeforeFixedUpdate, AsyncTaskBeforeUpdate, AsyncTaskContext,
        AsyncTaskPlugin, AsyncTaskSystems, SpawnAsyncTaskDeferredExt, SpawnAsyncTaskExt, WorldTask,
        WorldTaskDelay, WorldTaskLoop, WorldTaskSchedule,
        event::{EntityEventFutureExt, EventStreamTaskExt},
        message::MessageStreamTaskExt,
    };
}

//==================================================================================================
// AsyncTaskPlugin
//==================================================================================================

pub struct AsyncTaskPlugin;

#[derive(ScheduleLabel, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskBeforeFixedUpdate;

#[derive(ScheduleLabel, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskBeforeUpdate;

#[derive(SystemSet, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncTaskSystems;

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
                fixed_update_world_tasks.in_set(AsyncTaskSystems),
            )
            .add_systems(FixedLast, receive_world_tasks.in_set(AsyncTaskSystems))
            .add_systems(
                AsyncTaskBeforeUpdate,
                update_world_tasks.in_set(AsyncTaskSystems),
            )
            .add_systems(Last, receive_world_tasks.in_set(AsyncTaskSystems));
    }
}

fn fixed_update_world_tasks(world: &mut World) {
    let fixed_time = world.resource::<Time<Fixed>>();
    let delta = fixed_time.delta();

    let mut cx = world.resource_mut::<AsyncContext>();
    cx.fixed_update_delayed_world_tasks(delta);

    while AsyncContext::run_fixed_update_world_tasks(world) {}
}

fn update_world_tasks(world: &mut World) {
    let time = world.resource::<Time>();
    let delta = time.delta();

    let mut cx = world.resource_mut::<AsyncContext>();
    cx.update_delayed_world_tasks(delta);

    while AsyncContext::run_update_world_tasks(world) {}
}

fn receive_world_tasks(mut cx: ResMut<AsyncContext>) {
    cx.receive_world_tasks();
}

//==================================================================================================
// WorldTask
//==================================================================================================

#[must_use = "futures do nothing unless you `.await`, `.detach()` or poll them"]
pub struct WorldTask<R = ()> {
    data: Option<WorldTaskData>,
    cx: AsyncTaskContext,
    result_rx: oneshot::Receiver<R>,
}

impl<R> Future for WorldTask<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(data) = this.data.take() {
            this.cx.send(data);
        }

        let pinned_result_rx = Pin::new(&mut this.result_rx);
        match pinned_result_rx.poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // Sender was dropped, most likely during app shutdown.
            // Ignore the disconnect and keep the future pending.
            Poll::Ready(Err(_)) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> WorldTask<R>
where
    R: Send + 'static,
{
    pub fn once<F>(cx: AsyncTaskContext, f: F) -> Self
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();

        Self {
            data: Some(WorldTaskData {
                task: WorldTaskFn::Once(Box::new(|world| {
                    let _ = result_tx.send(f(world));
                })),
                delay: WorldTaskDelay::default(),
                schedule: WorldTaskSchedule::default(),
            }),
            cx,
            result_rx,
        }
    }

    pub fn looped<F>(cx: AsyncTaskContext, mut f: F) -> Self
    where
        F: FnMut(&mut World) -> WorldTaskLoop<R> + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let mut result_tx = Some(result_tx);

        Self {
            data: Some(WorldTaskData {
                task: WorldTaskFn::Loop(Box::new(move |world| match f(world) {
                    WorldTaskLoop::Continue(delay) => Some(delay),
                    WorldTaskLoop::Return(result) => {
                        if let Some(result_tx) = result_tx.take() {
                            let _ = result_tx.send(result);
                        }
                        None
                    }
                })),
                delay: WorldTaskDelay::default(),
                schedule: WorldTaskSchedule::default(),
            }),
            cx,
            result_rx,
        }
    }

    pub fn delay(mut self, delay: WorldTaskDelay) -> Self {
        let data = self.data.as_mut().unwrap();
        data.delay = delay;
        self
    }

    pub fn schedule(mut self, schedule: WorldTaskSchedule) -> Self {
        let data = self.data.as_mut().unwrap();
        data.schedule = schedule;
        self
    }

    pub fn spawn(self) -> BoxFuture<'static, R> {
        self.cx.send(self.data.expect("task data missing"));
        async {
            match self.result_rx.await {
                Ok(result) => result,
                // Sender was dropped, most likely during app shutdown.
                // Ignore the disconnect and keep the future pending.
                Err(_) => std::future::pending::<R>().await,
            }
        }
        .boxed()
    }

    pub fn detach(self) {
        self.cx.send(self.data.expect("task data missing"));
    }
}

//==================================================================================================
// WorldTaskData
//==================================================================================================

struct WorldTaskData {
    task: WorldTaskFn,
    delay: WorldTaskDelay,
    schedule: WorldTaskSchedule,
}

//==================================================================================================
// WorldTaskFn
//==================================================================================================

#[allow(clippy::type_complexity)]
enum WorldTaskFn {
    Once(Box<dyn FnOnce(&mut World) + Send + Sync>),
    Loop(Box<dyn FnMut(&mut World) -> Option<WorldTaskDelay> + Send + Sync>),
}

//==================================================================================================
// WorldTaskDelay
//==================================================================================================

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WorldTaskDelay {
    #[default]
    None,
    Ticks(usize),
    Elapsed(Duration),
}

impl WorldTaskDelay {
    pub fn secs(secs: f64) -> Self {
        Self::Elapsed(checked_duration_from_secs_f64(secs))
    }

    pub fn is_zero(&self) -> bool {
        match self {
            Self::None => true,
            Self::Ticks(ticks) => *ticks == 0,
            Self::Elapsed(elapsed) => elapsed.is_zero(),
        }
    }
}

//==================================================================================================
// WorldTaskSchedule
//==================================================================================================

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WorldTaskSchedule {
    #[default]
    Update,
    FixedUpdate,
}

//==================================================================================================
// WorldTaskLoop
//==================================================================================================

pub enum WorldTaskLoop<R = ()> {
    /// If `[WorldTaskDelay::is_zero]` is ture, it will immediately start with the next iteration.
    Continue(WorldTaskDelay),
    Return(R),
}

impl<R: Clone> Clone for WorldTaskLoop<R> {
    fn clone(&self) -> Self {
        match self {
            Self::Continue(delay) => Self::Continue(*delay),
            Self::Return(result) => Self::Return(result.clone()),
        }
    }
}

impl<R: Copy> Copy for WorldTaskLoop<R> {}

//==================================================================================================
// AsyncContext
//==================================================================================================

#[derive(Resource)]
pub struct AsyncContext {
    update_world_tasks: Vec<Option<WorldTaskFn>>,
    fixed_update_world_tasks: Vec<Option<WorldTaskFn>>,
    delayed_update_world_tasks: Vec<(Option<WorldTaskFn>, WorldTaskDelay)>,
    delayed_fixed_update_world_tasks: Vec<(Option<WorldTaskFn>, WorldTaskDelay)>,
    world_task_tx: mpsc::UnboundedSender<WorldTaskData>,
    world_task_rx: mpsc::UnboundedReceiver<WorldTaskData>,
}

impl Drop for AsyncContext {
    fn drop(&mut self) {
        info!("AsyncContext dropped. Existing async operations may leak resources.");
    }
}

impl AsyncContext {
    fn new() -> Self {
        let (world_task_tx, world_task_rx) = mpsc::unbounded();

        Self {
            update_world_tasks: Vec::default(),
            fixed_update_world_tasks: Vec::default(),
            delayed_update_world_tasks: Vec::default(),
            delayed_fixed_update_world_tasks: Vec::default(),
            world_task_tx,
            world_task_rx,
        }
    }

    pub fn task_context(&self) -> AsyncTaskContext {
        AsyncTaskContext {
            world_task_tx: self.world_task_tx.clone(),
        }
    }

    fn run_update_world_tasks(world: &mut World) -> bool {
        let mut this = world.resource_mut::<Self>();

        let mut update_world_tasks = Vec::new();
        let mut delayed_update_world_tasks = Vec::new();

        mem::swap(&mut this.update_world_tasks, &mut update_world_tasks);
        mem::swap(
            &mut this.delayed_update_world_tasks,
            &mut delayed_update_world_tasks,
        );

        Self::run_world_tasks_helper(
            world,
            &mut update_world_tasks,
            &mut delayed_update_world_tasks,
        );

        let mut this = world.resource_mut::<Self>();

        mem::swap(&mut this.update_world_tasks, &mut update_world_tasks);
        mem::swap(
            &mut this.delayed_update_world_tasks,
            &mut delayed_update_world_tasks,
        );

        this.update_world_tasks.append(&mut update_world_tasks);
        this.delayed_update_world_tasks
            .append(&mut delayed_update_world_tasks);

        !this.update_world_tasks.is_empty()
    }

    fn run_fixed_update_world_tasks(world: &mut World) -> bool {
        let mut this = world.resource_mut::<Self>();

        let mut fixed_update_world_tasks = Vec::new();
        let mut delayed_fixed_update_world_tasks = Vec::new();

        mem::swap(
            &mut this.fixed_update_world_tasks,
            &mut fixed_update_world_tasks,
        );
        mem::swap(
            &mut this.delayed_fixed_update_world_tasks,
            &mut delayed_fixed_update_world_tasks,
        );

        Self::run_world_tasks_helper(
            world,
            &mut fixed_update_world_tasks,
            &mut delayed_fixed_update_world_tasks,
        );

        let mut this = world.resource_mut::<Self>();

        mem::swap(
            &mut this.fixed_update_world_tasks,
            &mut fixed_update_world_tasks,
        );
        mem::swap(
            &mut this.delayed_fixed_update_world_tasks,
            &mut delayed_fixed_update_world_tasks,
        );

        this.fixed_update_world_tasks
            .append(&mut fixed_update_world_tasks);
        this.delayed_fixed_update_world_tasks
            .append(&mut delayed_fixed_update_world_tasks);

        !this.fixed_update_world_tasks.is_empty()
    }

    fn run_world_tasks_helper(
        world: &mut World,
        world_tasks: &mut Vec<Option<WorldTaskFn>>,
        delayed_world_tasks: &mut Vec<(Option<WorldTaskFn>, WorldTaskDelay)>,
    ) {
        world_tasks.retain_mut(|task| {
            if let Some(task) = task.take() {
                match task {
                    WorldTaskFn::Once(f) => f(world),
                    WorldTaskFn::Loop(mut f) => {
                        while let Some(delay) = f(world) {
                            if !delay.is_zero() {
                                delayed_world_tasks.push((Some(WorldTaskFn::Loop(f)), delay));
                                break;
                            }
                        }
                    }
                }
            }

            task.is_some()
        })
    }

    fn update_delayed_world_tasks(&mut self, delta: Duration) {
        Self::update_delayed_world_tasks_helper(
            &mut self.update_world_tasks,
            &mut self.delayed_update_world_tasks,
            delta,
        )
    }

    fn fixed_update_delayed_world_tasks(&mut self, delta: Duration) {
        Self::update_delayed_world_tasks_helper(
            &mut self.fixed_update_world_tasks,
            &mut self.delayed_fixed_update_world_tasks,
            delta,
        )
    }

    fn update_delayed_world_tasks_helper(
        world_tasks: &mut Vec<Option<WorldTaskFn>>,
        delayed_world_tasks: &mut Vec<(Option<WorldTaskFn>, WorldTaskDelay)>,
        delta: Duration,
    ) {
        delayed_world_tasks.retain_mut(|(task, delay)| {
            match delay {
                WorldTaskDelay::None => (),
                WorldTaskDelay::Ticks(ticks) => *ticks = ticks.saturating_sub(1),
                WorldTaskDelay::Elapsed(elapsed) => *elapsed = elapsed.saturating_sub(delta),
            }

            if delay.is_zero()
                && let Some(task) = task.take()
            {
                world_tasks.push(Some(task));
            }

            task.is_some()
        });
    }

    fn receive_world_tasks(&mut self) {
        while let Ok(world_task) = self.world_task_rx.try_recv() {
            let WorldTaskData {
                task,
                delay,
                schedule,
            } = world_task;

            match schedule {
                WorldTaskSchedule::Update => {
                    if delay.is_zero() {
                        self.update_world_tasks.push(Some(task));
                    } else {
                        self.delayed_update_world_tasks.push((Some(task), delay));
                    }
                }
                WorldTaskSchedule::FixedUpdate => {
                    if delay.is_zero() {
                        self.fixed_update_world_tasks.push(Some(task));
                    } else {
                        self.delayed_fixed_update_world_tasks
                            .push((Some(task), delay));
                    }
                }
            };
        }
    }
}

//==================================================================================================
// AsyncTaskContext
//==================================================================================================

#[derive(Clone)]
pub struct AsyncTaskContext {
    world_task_tx: mpsc::UnboundedSender<WorldTaskData>,
}

impl AsyncTaskContext {
    pub fn world_task_once<F, R>(&self, f: F) -> WorldTask<R>
    where
        F: FnOnce(&mut World) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        WorldTask::once(self.clone(), f)
    }

    pub fn world_task_looped<F, R>(&self, f: F) -> WorldTask<R>
    where
        F: FnMut(&mut World) -> WorldTaskLoop<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        WorldTask::looped(self.clone(), f)
    }

    pub fn delay(&self, delay: WorldTaskDelay) -> WorldTask<()> {
        WorldTask::once(self.clone(), |_| {}).delay(delay)
    }

    fn send(&self, world_task: WorldTaskData) {
        let _ = self.world_task_tx.unbounded_send(world_task);
    }
}

//==================================================================================================
// SpawnAsyncTaskExt
//==================================================================================================

pub trait SpawnAsyncTaskExt {
    fn spawn_async_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static;
}

impl SpawnAsyncTaskExt for World {
    fn spawn_async_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let cx = self.resource::<AsyncContext>().task_context();
        AsyncComputeTaskPool::get().spawn(task(cx))
    }
}

impl SpawnAsyncTaskExt for AsyncContext {
    fn spawn_async_task<T, F, R>(&self, task: T) -> Task<R>
    where
        T: FnOnce(AsyncTaskContext) -> F,
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let cx = self.task_context();
        AsyncComputeTaskPool::get().spawn(task(cx))
    }
}

impl SpawnAsyncTaskExt for AsyncTaskContext {
    fn spawn_async_task<T, F, R>(&self, task: T) -> Task<R>
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
// SpawnAsyncTaskDeferredExt
//==================================================================================================

pub trait SpawnAsyncTaskDeferredExt {
    fn spawn_async_task<T, F>(&mut self, task: T)
    where
        T: FnOnce(AsyncTaskContext) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static;
}

impl SpawnAsyncTaskDeferredExt for Commands<'_, '_> {
    fn spawn_async_task<T, F>(&mut self, task: T)
    where
        T: FnOnce(AsyncTaskContext) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        self.queue(move |world: &mut World| {
            world.spawn_async_task(task).detach();
        });
    }
}

//==================================================================================================
// helper functions
//==================================================================================================

fn checked_duration_from_secs_f64(secs: f64) -> Duration {
    debug_assert!(secs >= 0.0, "Duration must be non-negative");
    Duration::from_secs_f64(secs.max(0.0))
}
