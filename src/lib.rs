//! # Spawn async task:
//! ```rust
//! use bevy::{prelude::*, tasks::Task};
//! use bevy_mod_async::prelude::*;
//!
//! fn my_sync_work(world: &mut World) {
//!     // Works for [`World`], [`AsyncContext`], [`AsyncTaskContext`]:
//!     let task: Task<()> = world.spawn_async_task(async |cx: AsyncTaskContext| {});
//!     task.detach();
//!
//!     // Works for [`Commands`]:
//!     let mut commands = world.commands();
//!     commands.spawn_async_task(async |cx: AsyncTaskContext| {}); // Calls detach deferred.
//! }
//! ```
//!
//! # Access `&mut World` in the async task:
//! ```rust
//! use bevy::prelude::*;
//! use bevy_mod_async::prelude::*;
//! use std::time::Duration;
//!
//! async fn my_async_work(cx: AsyncTaskContext) {
//!     // Working with [`WorldTask`] once.
//!     let entity_count = cx.world_task(|world| world.entity_count())
//!         // Delay the task:
//!         // Delays are app loop dependent and have the same timing issue like simple app loop
//!         // timers!
//!         // Any delay where [`WorldTaskDelay::is_zero`] is false is guaranteed to delay atleast
//!         // one loop iteration.
//!         .delay(WorldTaskDelay::None) // The default
//!         .delay(WorldTaskDelay::Ticks(1)) // At least 1 update loop iter must complete.
//!         .delay(WorldTaskDelay::Elapsed(Duration::from_secs(1)))
//!         .delay(WorldTaskDelay::secs(2.1))
//!         // Specify the the schedule:
//!         .schedule(WorldTaskSchedule::Update) // The default
//!         .schedule(WorldTaskSchedule::FixedUpdate)
//!         // The [`WorldTask`] is a future that starts with `.await`, `.spawn()`, `.detach()` or
//!         // if you poll it.
//!         //.spawn() // Start the task and give us the receiver (can awaited later).
//!         //.detach(); // Start the task, we don't want to wait for it.
//!         .await; // Start now and wait for the result.
//!     info!("World contains {entity_count} entities.");
//!
//!     // Working with [`WorldTask`] loop.
//!     // You can configure them like above.
//!     let mut i = 0;
//!     let count = cx.world_task_looped(move |world| {
//!         i += 1;
//!         if i >= 10 {
//!             // End the loop and return a result.
//!             return WorldTaskLoop::Return(i);
//!         }
//!         // Continue the result after a delay.
//!         // Delays are app loop dependent and have the same timing issue like simple app loop
//!         // timers!
//!         WorldTaskLoop::Continue(WorldTaskDelay::secs(2.1))
//!     }).await;
//!     info!("Looped {count} time(s).");
//! }
//! ```
//!
//! # Awaiting [`Event`], [`EntityEvent`] and [`Message`].
//! ```rust
//! use bevy::prelude::*;
//! use bevy_mod_async::prelude::*;
//!
//! #[derive(Event, Clone)]
//! struct SomeEvent;
//!
//! #[derive(EntityEvent, Clone)]
//! struct SomeEntityEvent(Entity);
//!
//! #[derive(Message, Clone)]
//! struct SomeMessage(String);
//!
//! async fn my_async_work(cx: AsyncTaskContext, e0: Entity, e1: Entity, e2: Entity) {
//!     // Await event stream.
//!     // The stream starts at creation time and misses earlier events.
//!     let mut events = cx.world_task(SomeEvent::event_stream).await;
//!     for _ in 0..10 {
//!         let _: SomeEvent = events.next_event().await;
//!     }
//!     info!("Received 10 SomeEvents (EventStream).");
//!
//!     // Await single event.
//!     // The stream starts at creation time and misses earlier events.
//!     let event = cx.world_task(SomeEvent::to_future).await;
//!     let _: SomeEvent = event.await;
//!     info!("SomeEvent occured.");
//!
//!     // Await entity event stream.
//!     // The stream starts at creation time and misses earlier events.
//!     let mut entity_events = cx.world_task(move |world| {
//!         let _ = [e0, e1, e2].event_stream::<SomeEntityEvent>(world); // Multiple entities
//!         e0.event_stream::<SomeEntityEvent>(world) // Single entity
//!     }).await;
//!     for _ in 0..10 {
//!         let _: SomeEntityEvent = entity_events.next_event().await;
//!     }
//!     info!("Received 10 SomeEntityEvents (EventStream).");
//!
//!     // Await single entity event.
//!     // The stream starts at creation time and misses earlier events.
//!     let entity_event = cx.world_task(move |world| {
//!         let _ = [e0, e1, e2].observe_future::<SomeEntityEvent>(world); // Multiple entities
//!         e0.observe_future::<SomeEntityEvent>(world) // Single entity
//!     }).await;
//!     let _: SomeEntityEvent = entity_event.await;
//!     info!("SomeEntityEvent occured.");
//!
//!     // Await message stream.
//!     // The stream starts at creation time and may miss earlier messages.
//!     let mut messages = cx.world_task(|world| SomeMessage::message_stream(world)).await;
//!     for _ in 0..10 {
//!         let some_message: SomeMessage = messages.next_message().await;
//!         info!("Received message (MessageStream): {}", some_message.0);
//!     }
//!
//!     // Await single message.
//!     let message = cx.world_task(|world| SomeMessage::to_future(world)).await;
//!     info!("Received message: {}", message.await.0);
//! }
//! ```
//!
//! # Example for waiting for assets:
//! ```rust
//! use bevy::prelude::*;
//! use bevy_mod_async::prelude::*;
//!
//! fn my_startup_system(mut commands: Commands, asset_server: Res<AssetServer>) {
//!     let background: Handle<Image> = asset_server.load("background.png");
//!     let logo: Handle<Image> = asset_server.load("logo.png");
//!
//!     let asset_server = asset_server.clone();
//!     let assets = [
//!         background.clone().untyped(),
//!         logo.clone().untyped(),
//!     ];
//!     commands.spawn_async_task(async move |cx| {
//!         wait_for_assets(&asset_server, assets).await;
//!         cx.world_task(|world| {
//!             world.spawn(Sprite::from_image(background));
//!             world.spawn((
//!                 Sprite::from_image(logo),
//!                 Transform::from_xyz(0.0, 0.0, 1.0),
//!             ));
//!         }).await;
//!     });
//! }
//!
//! async fn wait_for_assets(
//!     asset_server: &AssetServer,
//!     assets: impl IntoIterator<Item = UntypedHandle>,
//! ) {
//!     for asset in assets {
//!         if let Err(e) = asset_server.wait_for_asset_untyped(&asset).await {
//!             error!("Failed to load audio: {e}");
//!         };
//!     }
//! }
//! ```
//!
//! # App loop usage:
//! This is just an example you can abstract things in better organized structures or systems.
//! ```rust
//! use bevy::prelude::*;
//! use bevy_mod_async::prelude::*;
//!
//! #[derive(EntityEvent, Clone)]
//! struct ButtonUp(Entity);
//!
//! async fn app_loop(
//!     cx: &AsyncTaskContext,
//!     animation_player: Entity,
//!     play_button: Entity,
//!     settings_button: Entity,
//! ) {
//!     animation_player_wait_all_finished(cx, animation_player).await;
//!     let button_up = read_input(cx, play_button, settings_button).await;
//!     if button_up == play_button {
//!         // play
//!     } else if button_up == settings_button {
//!         // settings
//!     }
//! }
//!
//! async fn animation_player_wait_all_finished(cx: &AsyncTaskContext, entity: Entity) {
//!     cx.world_task_looped(move |world| {
//!         let all_finished = world
//!             .get::<AnimationPlayer>(entity)
//!             .map(|animation_player| animation_player.all_finished())
//!             .unwrap_or_default();
//!
//!         if all_finished {
//!             return WorldTaskLoop::Return(());
//!         }
//!
//!         WorldTaskLoop::Continue(WorldTaskDelay::Ticks(1))
//!     }).await
//! }
//!
//! async fn read_input(
//!     cx: &AsyncTaskContext,
//!     play_button: Entity,
//!     settings_button: Entity,
//! ) -> Entity {
//!     // this could also be done with futures::select!
//!     let button_up_fut = cx.world_task(move |world| {
//!         [play_button, settings_button].observe_future::<ButtonUp>(world)
//!     }).await;
//!     button_up_fut.await.0
//! }
//! ```

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
            .add_systems(
                FixedLast,
                fixed_update_receive_world_tasks.in_set(AsyncTaskSystems),
            )
            .add_systems(
                AsyncTaskBeforeUpdate,
                update_world_tasks.in_set(AsyncTaskSystems),
            )
            .add_systems(Last, update_receive_world_tasks.in_set(AsyncTaskSystems));
    }
}

fn fixed_update_world_tasks(world: &mut World) {
    let fixed_time = world.resource::<Time<Fixed>>();
    let delta = fixed_time.delta();
    AsyncContext::update(world, delta, |cx| &mut cx.fixed_update_pipeline);
}

fn fixed_update_receive_world_tasks(mut cx: ResMut<AsyncContext>) {
    cx.fixed_update_pipeline.receive();
}

fn update_world_tasks(world: &mut World) {
    let time = world.resource::<Time>();
    let delta = time.delta();
    AsyncContext::update(world, delta, |cx| &mut cx.update_pipeline);
}

fn update_receive_world_tasks(mut cx: ResMut<AsyncContext>) {
    cx.update_pipeline.receive();
}

//==================================================================================================
// WorldTask
//==================================================================================================

#[must_use = "futures do nothing unless you `.await`, `.spawn()`, `.detach()` or poll them"]
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
    update_pipeline: WorldTaskPipeline,
    fixed_update_pipeline: WorldTaskPipeline,
}

impl Drop for AsyncContext {
    fn drop(&mut self) {
        info!("AsyncContext dropped. Existing async operations may leak resources.");
    }
}

impl AsyncContext {
    fn new() -> Self {
        Self {
            update_pipeline: WorldTaskPipeline::new(),
            fixed_update_pipeline: WorldTaskPipeline::new(),
        }
    }

    pub fn task_context(&self) -> AsyncTaskContext {
        AsyncTaskContext {
            update_world_task_tx: self.update_pipeline.sender.clone(),
            fixed_update_world_task_tx: self.fixed_update_pipeline.sender.clone(),
        }
    }

    fn update(
        world: &mut World,
        delta: Duration,
        cx_pipeline: fn(&mut AsyncContext) -> &mut WorldTaskPipeline,
    ) {
        let mut cx = world.resource_mut::<AsyncContext>();
        let pipeline = cx_pipeline(&mut cx);
        // Don't receive before updating delays to ensure there is at least 1 frame done.
        pipeline.update_delays(delta);

        loop {
            let mut cx = world.resource_mut::<AsyncContext>();
            let pipeline = cx_pipeline(&mut cx);
            pipeline.receive();

            WorldTaskPipeline::drain_world_tasks(world, cx_pipeline);

            let mut cx = world.resource_mut::<AsyncContext>();
            let pipeline = cx_pipeline(&mut cx);
            if !pipeline.needs_to_drain_world_tasks() {
                break;
            }
        }
    }
}

//==================================================================================================
// WorldTaskPipeline
//==================================================================================================

struct WorldTaskPipeline {
    world_tasks: Vec<Option<WorldTaskFn>>,
    delayed_world_tasks: Vec<(Option<WorldTaskFn>, WorldTaskDelay)>,
    sender: mpsc::UnboundedSender<(Option<WorldTaskFn>, WorldTaskDelay)>,
    receiver: mpsc::UnboundedReceiver<(Option<WorldTaskFn>, WorldTaskDelay)>,
}

impl WorldTaskPipeline {
    fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded();

        Self {
            world_tasks: Vec::default(),
            delayed_world_tasks: Vec::default(),
            sender,
            receiver,
        }
    }

    fn receive(&mut self) {
        while let Ok((Some(task), delay)) = self.receiver.try_recv() {
            if delay.is_zero() {
                self.world_tasks.push(Some(task));
            } else {
                self.delayed_world_tasks.push((Some(task), delay));
            }
        }
    }

    fn update_delays(&mut self, delta: Duration) {
        self.delayed_world_tasks.retain_mut(|(task, delay)| {
            match delay {
                WorldTaskDelay::None => (),
                WorldTaskDelay::Ticks(ticks) => *ticks = ticks.saturating_sub(1),
                WorldTaskDelay::Elapsed(elapsed) => *elapsed = elapsed.saturating_sub(delta),
            }

            if delay.is_zero()
                && let Some(task) = task.take()
            {
                self.world_tasks.push(Some(task));
            }

            task.is_some()
        })
    }

    fn needs_to_drain_world_tasks(&self) -> bool {
        !self.world_tasks.is_empty()
    }

    fn drain_world_tasks(
        world: &mut World,
        cx_pipeline: fn(&mut AsyncContext) -> &mut WorldTaskPipeline,
    ) {
        let mut cx = world.resource_mut::<AsyncContext>();

        let mut world_tasks = Vec::new();
        let mut delayed_world_tasks = Vec::new();

        let pipeline = cx_pipeline(&mut cx);
        mem::swap(&mut pipeline.world_tasks, &mut world_tasks);
        mem::swap(&mut pipeline.delayed_world_tasks, &mut delayed_world_tasks);

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
        });

        let mut cx = world.resource_mut::<AsyncContext>();

        let pipeline = cx_pipeline(&mut cx);
        mem::swap(&mut pipeline.world_tasks, &mut world_tasks);
        mem::swap(&mut pipeline.delayed_world_tasks, &mut delayed_world_tasks);

        pipeline.world_tasks.append(&mut world_tasks);
        pipeline
            .delayed_world_tasks
            .append(&mut delayed_world_tasks);
    }
}

//==================================================================================================
// AsyncTaskContext
//==================================================================================================

#[derive(Clone)]
pub struct AsyncTaskContext {
    update_world_task_tx: mpsc::UnboundedSender<(Option<WorldTaskFn>, WorldTaskDelay)>,
    fixed_update_world_task_tx: mpsc::UnboundedSender<(Option<WorldTaskFn>, WorldTaskDelay)>,
}

impl AsyncTaskContext {
    pub fn world_task<F, R>(&self, f: F) -> WorldTask<R>
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
        let WorldTaskData {
            task,
            delay,
            schedule,
        } = world_task;

        let sender = match schedule {
            WorldTaskSchedule::Update => &self.update_world_task_tx,
            WorldTaskSchedule::FixedUpdate => &self.fixed_update_world_task_tx,
        };

        let _ = sender.unbounded_send((Some(task), delay));
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
