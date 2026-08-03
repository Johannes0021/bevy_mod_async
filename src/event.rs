use crate::{AsyncContext, AsyncTaskContext, send_with_error_api_guard};
use bevy_ecs::{
    bundle::Bundle,
    component::Component,
    entity::Entity,
    event::{EntityEvent, Event},
    lifecycle::Remove,
    observer::{Observer, On},
    world::World,
};
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
        EventStream::new(world, [])
    }

    fn event_stream_with_bundle<B>(world: &mut World) -> EventStream<Self, B>
    where
        B: Bundle,
    {
        EventStream::new(world, [])
    }
}

impl<T> EventStreamTaskExt for T where T: Event + Clone {}

//==================================================================================================
// EntityEventFutureExt
//==================================================================================================

pub trait EntityEventFutureExt: Sized {
    fn into_event_future_target_entities(self) -> impl IntoIterator<Item = Entity>;

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

    fn event_stream<E>(self, world: &mut World) -> EventStream<E>
    where
        E: EntityEvent + Clone,
    {
        EventStream::new(world, self.into_event_future_target_entities())
    }

    fn event_stream_with_bundle<E, B>(self, world: &mut World) -> EventStream<E, B>
    where
        E: EntityEvent + Clone,
        B: Bundle,
    {
        EventStream::new(world, self.into_event_future_target_entities())
    }
}

impl EntityEventFutureExt for Entity {
    fn into_event_future_target_entities(self) -> impl IntoIterator<Item = Entity> {
        [self]
    }
}

impl<T, const N: usize> EntityEventFutureExt for [T; N]
where
    T: Into<Entity>,
{
    fn into_event_future_target_entities(self) -> impl IntoIterator<Item = Entity> {
        self.into_iter().map(Into::into)
    }
}

impl<T> EntityEventFutureExt for &[T]
where
    T: Into<Entity> + Clone,
{
    fn into_event_future_target_entities(self) -> impl IntoIterator<Item = Entity> {
        self.iter().cloned().map(Into::into)
    }
}

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
                // Sender was dropped, most likely during app shutdown.
                // Ignore the disconnect and keep the stream pending.

                let this = self.get_mut();
                this.ensure_observer_is_scheduled_to_despawn();

                Poll::Pending
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
    pub fn new<I>(world: &mut World, entities: I) -> Self
    where
        I: IntoIterator<Item = Entity>,
    {
        #[derive(Component)]
        struct EventFutureDespawnMarker;

        let waker_tx = Arc::new(AtomicWaker::new());
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let cx = world.resource::<AsyncContext>().create_task_context();

        let waker_rx = waker_tx.clone();
        let event_tx_clone = event_tx.clone();
        let mut observer = world.spawn(
            Observer::new(move |event: On<E, B>| {
                send_with_error_api_guard(&event_tx_clone, Ok(event.event().clone()), None);
                waker_rx.wake();
            })
            .with_entities(entities),
        );

        let waker_rx = waker_tx.clone();
        observer.observe(move |event: On<Remove, EventFutureDespawnMarker>| {
            send_with_error_api_guard(
                &event_tx,
                Err(EventFutureError::TrackingMarkerRemoved {
                    entity: event.event().entity,
                }),
                None,
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
