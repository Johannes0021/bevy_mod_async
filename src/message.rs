use super::{mpsc_unbounded_send_with_error_api_guard, prelude::*};
use bevy_ecs::{message::MessageCursor, prelude::*};
use futures::{channel::mpsc, future::BoxFuture, prelude::*};
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
    message_rx: Box<mpsc::UnboundedReceiver<M>>,
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
        let this = self.get_mut();
        let pinned_message_rx = Pin::new(&mut this.message_rx);
        match pinned_message_rx.poll_next(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            // Sender was dropped, most likely during app shutdown.
            // Ignore the disconnect and keep the stream pending.
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
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
            message_tx: mpsc::UnboundedSender<MInner>,
            mut reader: MessageCursor<MInner>,
            cx: AsyncTaskContext,
        ) where
            MInner: Message + Clone,
        {
            if quit_rx.load(Ordering::Relaxed) {
                return;
            }

            for message in reader.read(world.resource::<Messages<MInner>>()) {
                mpsc_unbounded_send_with_error_api_guard(&message_tx, message.clone(), None);
            }

            let cx_clone = cx.clone();
            cx.with_world_scheduled(RunAfter::UpdateTicks(1), move |world| {
                read_and_reschedule(world, quit_rx, message_tx, reader, cx_clone);
            })
            .detach();
        }

        let quit_tx = Arc::new(AtomicBool::new(false));
        let quit_rx = quit_tx.clone();

        let (message_tx, message_rx) = mpsc::unbounded();

        let reader = MessageCursor::default();

        let cx = world.resource::<AsyncContext>().create_task_context();
        let cx_clone = cx.clone();
        cx.with_world(move |world| {
            read_and_reschedule::<M>(world, quit_rx, message_tx, reader, cx_clone);
        })
        .detach();

        Self {
            quit_tx,
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
