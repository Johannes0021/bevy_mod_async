use crate::{AsyncContext, AsyncTaskContext, RunAfter, send_with_error_api_guard};
use bevy_ecs::{
    message::{Message, MessageCursor, Messages},
    world::World,
};
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, task::AtomicWaker};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

//==================================================================================================
// MessageStreamTaskExt
//==================================================================================================

pub trait MessageStreamTaskExt: Message + Clone {
    fn to_future(world: &World) -> BoxFuture<'static, Self> {
        let mut stream = Self::message_stream(world);
        async move { stream.next_message().await }.boxed()
    }

    fn message_stream(world: &World) -> MessageStream<Self> {
        MessageStream::new(world)
    }
}

impl<T> MessageStreamTaskExt for T where T: Message + Clone {}

//==================================================================================================
// MessageStream
//==================================================================================================

#[must_use]
pub struct MessageStream<M> {
    quit_tx: Arc<AtomicBool>,
    waker_tx: Arc<AtomicWaker>,
    message_rx: Box<crossbeam_channel::Receiver<M>>,
}

impl<M> Drop for MessageStream<M> {
    fn drop(&mut self) {
        self.quit_tx.store(true, Ordering::Relaxed);
    }
}

impl<M> Stream for MessageStream<M>
where
    M: Message + Clone,
{
    type Item = M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker_tx.register(cx.waker());

        match self.message_rx.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),
            Err(crossbeam_channel::TryRecvError::Empty) => Poll::Pending,
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                panic!("Failed to receive message. Did you remove `AsyncContext` resource?",)
            }
        }
    }
}

impl<M> MessageStream<M>
where
    M: Message + Clone,
{
    pub fn new(world: &World) -> Self {
        fn read_and_reschedule<MInner>(
            world: &mut World,
            quit_rx: Arc<AtomicBool>,
            waker_rx: Arc<AtomicWaker>,
            message_tx: crossbeam_channel::Sender<MInner>,
            mut reader: MessageCursor<MInner>,
            cx: AsyncTaskContext,
        ) where
            MInner: Message + Clone,
        {
            if quit_rx.load(Ordering::Relaxed) {
                return;
            }

            for message in reader.read(world.resource::<Messages<MInner>>()) {
                send_with_error_api_guard(&message_tx, message.clone())
            }

            waker_rx.wake();

            let cx_clone = cx.clone();
            cx.with_world_scheduled(RunAfter::UpdateTicks(1), move |world| {
                read_and_reschedule(world, quit_rx, waker_rx, message_tx, reader, cx_clone);
            })
            .detach();
        }

        let quit_tx = Arc::new(AtomicBool::new(false));
        let quit_rx = quit_tx.clone();

        let waker_tx = Arc::new(AtomicWaker::new());
        let waker_rx = waker_tx.clone();

        let (message_tx, message_rx) = crossbeam_channel::unbounded();

        let reader = MessageCursor::default();

        let cx = world.resource::<AsyncContext>().create_task_context();
        let cx_clone = cx.clone();
        cx.with_world(move |world| {
            read_and_reschedule::<M>(world, quit_rx, waker_rx, message_tx, reader, cx_clone);
        })
        .detach();

        Self {
            quit_tx,
            waker_tx,
            message_rx: Box::new(message_rx),
        }
    }
}

impl<M> MessageStream<M>
where
    M: Message + Clone,
{
    pub async fn next_message(&mut self) -> M {
        match self.next().await {
            Some(v) => v,
            // This should be unreachable in this design,
            // but must be handled because Stream requires Option.
            None => unreachable!(),
        }
    }
}
