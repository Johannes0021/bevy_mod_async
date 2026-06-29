use crate::{AsyncTaskContext, WithWorldFuture};
use bevy_ecs::message::{Message, MessageCursor, Messages};
use futures::{FutureExt, Stream, StreamExt};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

//==================================================================================================
// MessageStreamTaskExt
//==================================================================================================

pub trait MessageStreamTaskExt {
    fn message_stream<M: Message + Clone>(&self) -> MessageStream<M>;
}

impl MessageStreamTaskExt for AsyncTaskContext {
    fn message_stream<M: Message + Clone>(&self) -> MessageStream<M> {
        MessageStream::<M>::new(self.clone())
    }
}

//==================================================================================================
// MessageFutureExt
//==================================================================================================

pub trait MessageFutureExt: Message + Clone {
    fn to_future(cx: &AsyncTaskContext) -> impl Future<Output = Self>
    where
        Self: Sized,
    {
        async { cx.message_stream().next_message().await }
    }
}

impl<T> MessageFutureExt for T where T: Message + Clone {}

//==================================================================================================
// MessageStreamData
//==================================================================================================

struct MessageStreamData<M: Message> {
    data: VecDeque<M>,
    reader: MessageCursor<M>,
}

impl<M: Message> Default for MessageStreamData<M> {
    fn default() -> Self {
        MessageStreamData {
            data: Default::default(),
            reader: Default::default(),
        }
    }
}

//==================================================================================================
// MessageStreamState
//==================================================================================================

enum MessageStreamState<M: Message> {
    HasData(MessageStreamData<M>),
    WaitingForTask(WithWorldFuture<Box<MessageStreamData<M>>>),
}

impl<M: Message> Default for MessageStreamState<M> {
    fn default() -> Self {
        Self::HasData(Default::default())
    }
}

//==================================================================================================
// MessageStream
//==================================================================================================

#[must_use]
pub struct MessageStream<M>
where
    M: Message,
{
    cx: AsyncTaskContext,
    state: Box<MessageStreamState<M>>,
}

impl<M: Message> MessageStream<M> {
    pub fn new(cx: AsyncTaskContext) -> Self {
        Self {
            cx,
            state: Default::default(),
        }
    }
}

impl<M> Stream for MessageStream<M>
where
    M: Message + Clone,
{
    type Item = M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut *this.state {
                MessageStreamState::HasData(data) => {
                    if let Some(next) = data.data.pop_front() {
                        return Poll::Ready(Some(next));
                    } else {
                        let mut reader = std::mem::take(&mut data.reader);
                        let waker = cx.waker().clone();
                        let fut = this.cx.with_world(move |world| {
                            let data = reader
                                .read(world.resource::<Messages<M>>())
                                .map(Clone::clone)
                                .collect::<VecDeque<_>>();

                            waker.wake();

                            Box::new(MessageStreamData { data, reader })
                        });
                        *this.state = MessageStreamState::WaitingForTask(fut);
                    }
                }
                MessageStreamState::WaitingForTask(fut) => {
                    if let Poll::Ready(data) = fut.poll_unpin(cx) {
                        *this.state = MessageStreamState::HasData(*data);
                    } else {
                        return Poll::Pending;
                    }
                }
            }
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
