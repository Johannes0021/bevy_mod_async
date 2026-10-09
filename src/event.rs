use crate::{AsyncContext, AsyncTaskContext, send_with_error_api_guard};
use bevy_ecs::{
    component::Component,
    entity::Entity,
    event::{EntityEvent, EventPattern},
    lifecycle::Remove,
    observer::{Observer, On},
    world::World,
};
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, task::AtomicWaker};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

//==================================================================================================
// EventStreamTaskExt
//==================================================================================================

pub trait EventStreamTaskExt: Sized + EventPattern
where
    Self::Event: Clone,
{
    fn to_future(world: &mut World) -> BoxFuture<'static, Self::Event> {
        let mut stream = Self::event_stream(world);
        async move { stream.next_event().await }.boxed()
    }

    fn event_stream(world: &mut World) -> EventStream<Self> {
        EventStream::new(world, [])
    }
}

impl<T> EventStreamTaskExt for T
where
    T: EventPattern,
    T::Event: Clone,
{
}

//==================================================================================================
// EntityEventFutureExt
//==================================================================================================

pub trait EntityEventFutureExt: Sized {
    fn into_event_future_target_entities(self) -> impl IntoIterator<Item = Entity>;

    fn observe_future<E>(self, world: &mut World) -> BoxFuture<'static, E::Event>
    where
        E: EventPattern,
        E::Event: EntityEvent + Clone,
    {
        let mut stream = self.event_stream::<E>(world);
        async move { stream.next_event().await }.boxed()
    }

    fn event_stream<E>(self, world: &mut World) -> EventStream<E>
    where
        E: EventPattern,
        E::Event: EntityEvent + Clone,
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

enum EventFutureError {
    TrackingMarkerRemoved,
}

//==================================================================================================
// EventStream
//==================================================================================================

#[must_use]
pub struct EventStream<E: EventPattern> {
    waker_tx: Arc<AtomicWaker>,
    event_rx: Box<crossbeam_channel::Receiver<Result<E::Event, EventFutureError>>>,
    cx: AsyncTaskContext,
    observer: Entity,
    observer_despawned: bool,
}

impl<E: EventPattern> Drop for EventStream<E> {
    fn drop(&mut self) {
        self.ensure_observer_is_scheduled_to_despawn();
    }
}

impl<E: EventPattern> Stream for EventStream<E> {
    type Item = E::Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_tx.register(cx.waker());

        match self.event_rx.try_recv() {
            Ok(Ok(v)) => Poll::Ready(Some(v)),

            Err(crossbeam_channel::TryRecvError::Empty) => Poll::Pending,

            Ok(Err(EventFutureError::TrackingMarkerRemoved))
            | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                // Sender was dropped, most likely during app shutdown.
                // Ignore the disconnect and keep the stream pending.

                let this = self.get_mut();
                this.ensure_observer_is_scheduled_to_despawn();

                Poll::Pending
            }
        }
    }
}

impl<E: EventPattern> EventStream<E> {
    pub async fn next_event(&mut self) -> E::Event {
        match self.next().await {
            Some(v) => v,
            // This should be unreachable in this design,
            // but must be handled because Stream requires Option.
            None => unreachable!(),
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

impl<E> EventStream<E>
where
    E: EventPattern,
    E::Event: Clone,
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
            Observer::new(move |event: On<E>| {
                send_with_error_api_guard(&event_tx_clone, Ok(event.event().clone()), None);
                waker_rx.wake();
            })
            .with_entities(entities),
        );

        let waker_rx = waker_tx.clone();
        observer.observe(move |_: On<Remove<EventFutureDespawnMarker>>| {
            send_with_error_api_guard(
                &event_tx,
                Err(EventFutureError::TrackingMarkerRemoved),
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
        }
    }
}
