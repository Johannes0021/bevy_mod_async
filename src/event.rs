use crate::{AsyncTaskContext, AsyncWork};
use bevy_ecs::prelude::*;
use futures::{Stream, StreamExt, task::AtomicWaker};
use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

//==================================================================================================
// EventStreamTaskExt
//==================================================================================================

pub trait EventStreamTaskExt {
    fn event_stream<E>(&self) -> impl Future<Output = EventStream<E>>
    where
        E: Event + Clone;

    fn event_stream_with_bundle<E, B>(&self) -> impl Future<Output = EventStream<E, B>>
    where
        E: Event + Clone,
        B: Bundle;
}

impl EventStreamTaskExt for AsyncTaskContext {
    fn event_stream<E>(&self) -> impl Future<Output = EventStream<E>>
    where
        E: Event + Clone,
    {
        self.with_world(EventStream::new)
    }

    fn event_stream_with_bundle<E, B>(&self) -> impl Future<Output = EventStream<E, B>>
    where
        E: Event + Clone,
        B: Bundle,
    {
        self.with_world(EventStream::new)
    }
}

//==================================================================================================
// EntityEventStreamTaskExt
//==================================================================================================

pub trait EntityEventStreamTaskExt {
    fn entity_event_stream<E>(&self, entity: Entity) -> impl Future<Output = EntityEventStream<E>>
    where
        E: EntityEvent + Clone;

    fn entity_event_stream_with_bundle<E, B>(
        &self,
        entity: Entity,
    ) -> impl Future<Output = EntityEventStream<E, B>>
    where
        E: EntityEvent + Clone,
        B: Bundle;
}

impl EntityEventStreamTaskExt for AsyncTaskContext {
    fn entity_event_stream<E>(&self, entity: Entity) -> impl Future<Output = EntityEventStream<E>>
    where
        E: EntityEvent + Clone,
    {
        self.with_world(move |w| EntityEventStream::new(w, entity))
    }

    fn entity_event_stream_with_bundle<E, B>(
        &self,
        entity: Entity,
    ) -> impl Future<Output = EntityEventStream<E, B>>
    where
        E: EntityEvent + Clone,
        B: Bundle,
    {
        self.with_world(move |w| EntityEventStream::new(w, entity))
    }
}

//==================================================================================================
// EventFutureExt
//==================================================================================================

pub trait EventFutureExt: Event + Clone {
    fn to_future(cx: &AsyncTaskContext) -> impl Future<Output = Result<Self, EventFutureError>>
    where
        Self: Sized,
    {
        async { cx.event_stream().await.next_event().await }
    }

    fn to_future_with_bundle<B>(
        cx: &AsyncTaskContext,
    ) -> impl Future<Output = Result<Self, EventFutureError>>
    where
        Self: Sized,
        B: Bundle,
    {
        async {
            cx.event_stream_with_bundle::<Self, B>()
                .await
                .next_event()
                .await
        }
    }
}

impl<T> EventFutureExt for T where T: Event + Clone {}

//==================================================================================================
// EntityEventFutureExt
//==================================================================================================

pub trait EntityEventFutureExt: Into<Entity> + Clone {
    fn observe_future<E>(
        self,
        cx: &AsyncTaskContext,
    ) -> impl Future<Output = Result<E, EventFutureError>>
    where
        Self: Sized,
        E: EntityEvent + Clone,
    {
        async { cx.entity_event_stream(self.into()).await.next_event().await }
    }

    fn observe_future_with_bundle<E, B>(
        self,
        cx: &AsyncTaskContext,
    ) -> impl Future<Output = Result<E, EventFutureError>>
    where
        Self: Sized,
        E: EntityEvent + Clone,
        B: Bundle,
    {
        async {
            cx.entity_event_stream_with_bundle::<E, B>(self.into())
                .await
                .next_event()
                .await
        }
    }
}

impl EntityEventFutureExt for Entity {}

//==================================================================================================
// EventFutureError
//==================================================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EventFutureError {
    /// The expected event could not complete because the tracking mechanism was removed before
    /// completion.
    ///
    /// If the observing entity has been despawned before the expected event was received,
    /// the future cannot complete successfully.
    /// This indicates a logic error or race condition in the event flow.
    TrackingMarkerRemoved { entity: Entity },
}

impl fmt::Debug for EventFutureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrackingMarkerRemoved { entity } => f
                .debug_struct("EventFutureError::MarkerRemoved")
                .field("entity", entity)
                .field(
                    "reason",
                    &"tracking marker was removed before event completion",
                )
                .finish(),
        }
    }
}

impl fmt::Display for EventFutureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrackingMarkerRemoved { entity } => {
                write!(
                    f,
                    "entity event failed: tracking marker was removed before completion ({})",
                    entity
                )
            }
        }
    }
}

//==================================================================================================
// EventStream
//==================================================================================================

/// Future that resolves when an event emits.
#[must_use = "future must be awaited to yield execution"]
pub struct EventStream<E, B = ()> {
    waker_tx: Arc<AtomicWaker>,
    result_rx: Box<crossbeam_channel::Receiver<Result<E, EventFutureError>>>,
    cx: AsyncTaskContext,
    observer: Entity,
    observer_despawned: bool,
    _bundle: PhantomData<fn() -> B>,
}

impl<E, B> Drop for EventStream<E, B> {
    fn drop(&mut self) {
        self.ensure_observer_is_scheduled_to_despawn();
    }
}

impl<E, B> Stream for EventStream<E, B> {
    type Item = Result<E, EventFutureError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_tx.register(cx.waker());

        match self.result_rx.try_recv() {
            Ok(Ok(v)) => Poll::Ready(Some(Ok(v))),

            Ok(Err(EventFutureError::TrackingMarkerRemoved { entity })) => {
                let this = self.get_mut();
                this.ensure_observer_is_scheduled_to_despawn();
                Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                    entity,
                })))
            }

            Err(crossbeam_channel::TryRecvError::Empty) => Poll::Pending,

            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                let this = self.get_mut();
                this.ensure_observer_is_scheduled_to_despawn();
                Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                    entity: this.observer,
                })))
            }
        }
    }
}

impl<E, B> EventStream<E, B> {
    fn ensure_observer_is_scheduled_to_despawn(&mut self) {
        if self.observer_despawned {
            return;
        }
        self.observer_despawned = true;

        let observer = self.observer;
        self.cx
            .with_world(move |world| {
                if let Ok(observer_mut) = world.get_entity_mut(observer) {
                    observer_mut.despawn()
                }
            })
            .detach();
    }
}

impl<E, B> EventStream<E, B> {
    pub async fn next_event(&mut self) -> Result<E, EventFutureError> {
        match self.next().await {
            Some(v) => v,
            // This should be unreachable in this design,
            // but must be handled because Stream requires Option
            None => Err(EventFutureError::TrackingMarkerRemoved {
                entity: self.observer,
            }),
        }
    }
}

impl<E, B> EventStream<E, B>
where
    E: Event + Clone,
    B: Bundle,
{
    pub fn new(world: &mut World) -> Self {
        #[derive(Component)]
        struct EventFutureDespawnMarker;

        let waker_tx = Arc::new(AtomicWaker::new());
        let (result_tx, result_rx) = crossbeam_channel::unbounded();
        let cx = world.resource::<AsyncWork>().create_task_context();

        let waker_rx = waker_tx.clone();
        let result_tx_clone = result_tx.clone();
        let mut observer = world.add_observer(move |event: On<E, B>| {
            send_with_error_api_guard(&result_tx_clone, Ok(event.event().clone()));
            waker_rx.wake();
        });

        observer.insert(EventFutureDespawnMarker);

        let waker_rx = waker_tx.clone();
        observer.observe(move |event: On<Remove, EventFutureDespawnMarker>| {
            send_with_error_api_guard(
                &result_tx,
                Err(EventFutureError::TrackingMarkerRemoved {
                    entity: event.event().entity,
                }),
            );
            waker_rx.wake();
        });

        Self {
            waker_tx,
            result_rx: Box::new(result_rx),
            cx,
            observer: observer.id(),
            observer_despawned: false,
            _bundle: PhantomData,
        }
    }
}

//==================================================================================================
// EntityEventStream
//==================================================================================================

/// Future that resolves when an event emits.
#[must_use = "future must be awaited to yield execution"]
pub struct EntityEventStream<E, B = ()> {
    waker_tx: Arc<AtomicWaker>,
    result_rx: Box<crossbeam_channel::Receiver<Result<E, EventFutureError>>>,
    entity: Entity,
    _bundle: PhantomData<fn() -> B>,
}

impl<E, B> Stream for EntityEventStream<E, B> {
    type Item = Result<E, EventFutureError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_tx.register(cx.waker());

        match self.result_rx.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),

            Err(crossbeam_channel::TryRecvError::Empty) => Poll::Pending,

            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                    entity: self.entity,
                })))
            }
        }
    }
}

impl<E, B> EntityEventStream<E, B> {
    pub async fn next_event(&mut self) -> Result<E, EventFutureError> {
        match self.next().await {
            Some(v) => v,
            // This should be unreachable in this design,
            // but must be handled because Stream requires Option
            None => Err(EventFutureError::TrackingMarkerRemoved {
                entity: self.entity,
            }),
        }
    }
}

impl<E, B> EntityEventStream<E, B>
where
    E: EntityEvent + Clone,
    B: Bundle,
{
    pub fn new(world: &mut World, entity: Entity) -> Self {
        #[derive(Component)]
        struct EntityEventFutureDespawnMarker;

        let waker_tx = Arc::new(AtomicWaker::new());
        let (result_tx, result_rx) = crossbeam_channel::unbounded();

        if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
            let waker_rx = waker_tx.clone();
            let result_tx_clone = result_tx.clone();
            entity_mut.observe(move |event: On<E, B>| {
                send_with_error_api_guard(&result_tx_clone, Ok(event.event().clone()));
                waker_rx.wake();
            });

            entity_mut.insert(EntityEventFutureDespawnMarker);

            let waker_rx = waker_tx.clone();
            entity_mut.observe(move |event: On<Remove, EntityEventFutureDespawnMarker>| {
                send_with_error_api_guard(
                    &result_tx,
                    Err(EventFutureError::TrackingMarkerRemoved {
                        entity: event.event().entity,
                    }),
                );
                waker_rx.wake();
            });
        } else {
            send_with_error_api_guard(
                &result_tx,
                Err(EventFutureError::TrackingMarkerRemoved { entity }),
            );
        }

        Self {
            waker_tx,
            result_rx: Box::new(result_rx),
            entity,
            _bundle: PhantomData,
        }
    }
}

impl<E, B> EntityEventStream<E, B> {
    pub fn entity(&self) -> Entity {
        self.entity
    }
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
fn send_with_error_api_guard<T>(tx: &crossbeam_channel::Sender<T>, value: T) {
    let result = tx.send(value);

    if let Err(crossbeam_channel::SendError(t)) = result {
        let _ = &t;
    }
}
