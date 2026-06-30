use crate::{AsyncContext, AsyncTaskContext, send_with_error_api_guard};
use bevy_ecs::prelude::*;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, task::AtomicWaker};
use std::{
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

//==================================================================================================
// EventStreamTaskExt
//==================================================================================================

pub trait EventStreamTaskExt: Event + Clone {
    fn to_future(world: &mut World) -> BoxFuture<'static, Result<Self, EventFutureError>> {
        let mut stream = Self::event_stream(world);
        async move { stream.next_event().await }.boxed()
    }

    fn to_future_with_bundle<B>(
        world: &mut World,
    ) -> BoxFuture<'static, Result<Self, EventFutureError>>
    where
        B: Bundle,
    {
        let mut stream = Self::event_stream_with_bundle::<B>(world);
        async move { stream.next_event().await }.boxed()
    }

    fn event_stream(world: &mut World) -> EventStream<Self> {
        EventStream::new(world)
    }

    fn event_stream_with_bundle<B>(world: &mut World) -> EventStream<Self, B>
    where
        B: Bundle,
    {
        EventStream::new(world)
    }
}

impl<T> EventStreamTaskExt for T where T: Event + Clone {}

//==================================================================================================
// EntityEventFutureExt
//==================================================================================================

pub trait EntityEventFutureExt: Into<Entity> + Clone {
    fn observe_future<E>(self, world: &mut World) -> BoxFuture<'static, Result<E, EventFutureError>>
    where
        E: EntityEvent + Clone,
    {
        let mut stream = self.event_stream(world);
        async move { stream.next_event().await }.boxed()
    }

    fn observe_future_with_bundle<E, B>(
        self,
        world: &mut World,
    ) -> BoxFuture<'static, Result<E, EventFutureError>>
    where
        E: EntityEvent + Clone,
        B: Bundle,
    {
        let mut stream = self.event_stream_with_bundle::<E, B>(world);
        async move { stream.next_event().await }.boxed()
    }

    fn event_stream<E>(self, world: &mut World) -> EntityEventStream<E>
    where
        E: EntityEvent + Clone,
    {
        EntityEventStream::new(world, self.into())
    }

    fn event_stream_with_bundle<E, B>(self, world: &mut World) -> EntityEventStream<E, B>
    where
        E: EntityEvent + Clone,
        B: Bundle,
    {
        EntityEventStream::new(world, self.into())
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
#[must_use]
pub struct EventStream<E, B = ()> {
    waker_tx: Arc<AtomicWaker>,
    event_rx: Box<crossbeam_channel::Receiver<Result<E, EventFutureError>>>,
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

        match self.event_rx.try_recv() {
            Ok(Ok(v)) => Poll::Ready(Some(Ok(v))),

            Ok(Err(EventFutureError::TrackingMarkerRemoved { entity })) => {
                let this = self.get_mut();
                this.ensure_observer_is_scheduled_to_despawn();
                Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                    entity,
                })))
            }

            Err(crossbeam_channel::TryRecvError::Empty) => {
                if self.observer_despawned {
                    Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                        entity: self.observer,
                    })))
                } else {
                    Poll::Pending
                }
            }

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
    pub async fn next_event(&mut self) -> Result<E, EventFutureError> {
        match self.next().await {
            Some(v) => v,
            // This should be unreachable in this design,
            // but must be handled because Stream requires Option.
            None => Err(EventFutureError::TrackingMarkerRemoved {
                entity: self.observer,
            }),
        }
    }

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

impl<E, B> EventStream<E, B>
where
    E: Event + Clone,
    B: Bundle,
{
    pub fn new(world: &mut World) -> Self {
        #[derive(Component)]
        struct EventFutureDespawnMarker;

        let waker_tx = Arc::new(AtomicWaker::new());
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let cx = world.resource::<AsyncContext>().create_task_context();

        let waker_rx = waker_tx.clone();
        let event_tx_clone = event_tx.clone();
        let mut observer = world.add_observer(move |event: On<E, B>| {
            send_with_error_api_guard(&event_tx_clone, Ok(event.event().clone()));
            waker_rx.wake();
        });

        let waker_rx = waker_tx.clone();
        observer.observe(move |event: On<Remove, EventFutureDespawnMarker>| {
            send_with_error_api_guard(
                &event_tx,
                Err(EventFutureError::TrackingMarkerRemoved {
                    entity: event.event().entity,
                }),
            );
            waker_rx.wake();
        });

        observer.insert(EventFutureDespawnMarker);

        Self {
            waker_tx,
            event_rx: Box::new(event_rx),
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
#[must_use]
pub struct EntityEventStream<E, B = ()> {
    waker_tx: Arc<AtomicWaker>,
    event_rx: Box<crossbeam_channel::Receiver<Result<E, EventFutureError>>>,
    entity: Entity,
    tracking_marker_removed: bool,
    _bundle: PhantomData<fn() -> B>,
}

impl<E, B> Stream for EntityEventStream<E, B> {
    type Item = Result<E, EventFutureError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_tx.register(cx.waker());

        match self.event_rx.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),

            Err(crossbeam_channel::TryRecvError::Empty) => {
                if self.tracking_marker_removed {
                    Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                        entity: self.entity,
                    })))
                } else {
                    Poll::Pending
                }
            }

            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                let this = self.get_mut();
                this.tracking_marker_removed = true;
                Poll::Ready(Some(Err(EventFutureError::TrackingMarkerRemoved {
                    entity: this.entity,
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
            // but must be handled because Stream requires Option.
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
        let (event_tx, event_rx) = crossbeam_channel::unbounded();

        let tracking_marker_removed = if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
            let waker_rx = waker_tx.clone();
            let event_tx_clone = event_tx.clone();
            entity_mut.observe(move |event: On<E, B>| {
                send_with_error_api_guard(&event_tx_clone, Ok(event.event().clone()));
                waker_rx.wake();
            });

            let waker_rx = waker_tx.clone();
            entity_mut.observe(move |event: On<Remove, EntityEventFutureDespawnMarker>| {
                send_with_error_api_guard(
                    &event_tx,
                    Err(EventFutureError::TrackingMarkerRemoved {
                        entity: event.event().entity,
                    }),
                );
                waker_rx.wake();
            });

            entity_mut.insert(EntityEventFutureDespawnMarker);

            false
        } else {
            send_with_error_api_guard(
                &event_tx,
                Err(EventFutureError::TrackingMarkerRemoved { entity }),
            );

            true
        };

        Self {
            waker_tx,
            event_rx: Box::new(event_rx),
            entity,
            tracking_marker_removed,
            _bundle: PhantomData,
        }
    }
}

impl<E, B> EntityEventStream<E, B> {
    pub fn entity(&self) -> Entity {
        self.entity
    }
}
