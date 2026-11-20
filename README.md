Run example:
```sh
cargo run --example hello_world
```

# Spawn async task:
```rust
use bevy::{prelude::*, tasks::Task};
use bevy_mod_async::prelude::*;

fn my_sync_work(world: &mut World) {
    // Works for [`World`], [`AsyncContext`], [`AsyncTaskContext`]:
    let task: Task<()> = world.spawn_async_task(async |cx: AsyncTaskContext| {});
    task.detach();

    // Works for [`Commands`]:
    let mut commands = world.commands();
    commands.spawn_async_task(async |cx: AsyncTaskContext| {}); // Calls detach deferred.
}
```

# Access `&mut World` in the async task:
```rust
use bevy::prelude::*;
use bevy_mod_async::prelude::*;
use std::time::Duration;

async fn my_async_work(cx: AsyncTaskContext) {
    // Working with [`WorldTask`] once.
    let entity_count = cx.world_task(|world| world.entity_count())
        // Delay the task:
        // Delays are app loop dependent and have the same timing issue like simple app loop
        // timers!
        // Any delay where [`WorldTaskDelay::is_zero`] is false is guaranteed to delay atleast
        // one loop iteration.
        .delay(WorldTaskDelay::None) // The default
        .delay(WorldTaskDelay::Ticks(1)) // At least 1 update loop iter must complete.
        .delay(WorldTaskDelay::Elapsed(Duration::from_secs(1)))
        .delay(WorldTaskDelay::secs(2.1))
        // Specify the the schedule:
        .schedule(WorldTaskSchedule::Update) // The default
        .schedule(WorldTaskSchedule::FixedUpdate)
        // The [`WorldTask`] is a future that starts with `.await`, `.spawn()`, `.detach()` or
        // if you poll it.
        //.spawn() // Start the task and give us the receiver (can awaited later).
        //.detach(); // Start the task, we don't want to wait for it.
        .await; // Start now and wait for the result.
    info!("World contains {entity_count} entities.");

    // Working with [`WorldTask`] loop.
    // You can configure them like above.
    let mut i = 0;
    let count = cx.world_task_looped(move |world| {
        i += 1;
        if i >= 10 {
            // End the loop and return a result.
            return WorldTaskLoop::Return(i);
        }
        // Continue the result after a delay.
        // Delays are app loop dependent and have the same timing issue like simple app loop
        // timers!
        WorldTaskLoop::Continue(WorldTaskDelay::secs(2.1))
    }).await;
    info!("Looped {count} time(s).");
}
```

# Awaiting [`Event`], [`EntityEvent`] and [`Message`].
```rust
use bevy::prelude::*;
use bevy_mod_async::prelude::*;

#[derive(Event, Clone)]
struct SomeEvent;

#[derive(EntityEvent, Clone)]
struct SomeEntityEvent(Entity);

#[derive(Message, Clone)]
struct SomeMessage(String);

async fn my_async_work(cx: AsyncTaskContext, e0: Entity, e1: Entity, e2: Entity) {
    // Await event stream.
    // The stream starts at creation time and misses earlier events.
    let mut events = cx.world_task(SomeEvent::event_stream).await;
    for _ in 0..10 {
        let _: SomeEvent = events.next_event().await;
    }
    info!("Received 10 SomeEvents (EventStream).");

    // Await single event.
    // The stream starts at creation time and misses earlier events.
    let event = cx.world_task(SomeEvent::to_future).await;
    let _: SomeEvent = event.await;
    info!("SomeEvent occured.");

    // Await entity event stream.
    // The stream starts at creation time and misses earlier events.
    let mut entity_events = cx.world_task(move |world| {
        let _ = [e0, e1, e2].event_stream::<SomeEntityEvent>(world); // Multiple entities
        e0.event_stream::<SomeEntityEvent>(world) // Single entity
    }).await;
    for _ in 0..10 {
        let _: SomeEntityEvent = entity_events.next_event().await;
    }
    info!("Received 10 SomeEntityEvents (EventStream).");

    // Await single entity event.
    // The stream starts at creation time and misses earlier events.
    let entity_event = cx.world_task(move |world| {
        let _ = [e0, e1, e2].observe_future::<SomeEntityEvent>(world); // Multiple entities
        e0.observe_future::<SomeEntityEvent>(world) // Single entity
    }).await;
    let _: SomeEntityEvent = entity_event.await;
    info!("SomeEntityEvent occured.");

    // Await message stream.
    // The stream starts at creation time and may miss earlier messages.
    let mut messages = cx.world_task(|world| SomeMessage::message_stream(world)).await;
    for _ in 0..10 {
        let some_message: SomeMessage = messages.next_message().await;
        info!("Received message (MessageStream): {}", some_message.0);
    }

    // Await single message.
    let message = cx.world_task(|world| SomeMessage::to_future(world)).await;
    info!("Received message: {}", message.await.0);
}
```

# Example for waiting for assets:
```rust
use bevy::prelude::*;
use bevy_mod_async::prelude::*;

fn my_startup_system(mut commands: Commands, asset_server: Res<AssetServer>) {
    let background: Handle<Image> = asset_server.load("background.png");
    let logo: Handle<Image> = asset_server.load("logo.png");

    let asset_server = asset_server.clone();
    let assets = [
        background.clone().untyped(),
        logo.clone().untyped(),
    ];
    commands.spawn_async_task(async move |cx| {
        wait_for_assets(&asset_server, assets).await;
        cx.world_task(|world| {
            world.spawn(Sprite::from_image(background));
            world.spawn((
                Sprite::from_image(logo),
                Transform::from_xyz(0.0, 0.0, 1.0),
            ));
        }).await;
    });
}

async fn wait_for_assets(
    asset_server: &AssetServer,
    assets: impl IntoIterator<Item = UntypedHandle>,
) {
    for asset in assets {
        if let Err(e) = asset_server.wait_for_asset_untyped(&asset).await {
            error!("Failed to load audio: {e}");
        };
    }
}
```

# App loop usage:
This is just an example you can abstract things in better organized structures or systems.
```rust
use bevy::prelude::*;
use bevy_mod_async::prelude::*;

#[derive(EntityEvent, Clone)]
struct ButtonUp(Entity);

async fn app_loop(
    cx: &AsyncTaskContext,
    animation_player: Entity,
    play_button: Entity,
    settings_button: Entity,
) {
    animation_player_wait_all_finished(cx, animation_player).await;
    let button_up = read_input(cx, play_button, settings_button).await;
    if button_up == play_button {
        // play
    } else if button_up == settings_button {
        // settings
    }
}

async fn animation_player_wait_all_finished(cx: &AsyncTaskContext, entity: Entity) {
    cx.world_task_looped(move |world| {
        let all_finished = world
            .get::<AnimationPlayer>(entity)
            .map(|animation_player| animation_player.all_finished())
            .unwrap_or_default();

        if all_finished {
            return WorldTaskLoop::Return(());
        }

        WorldTaskLoop::Continue(WorldTaskDelay::Ticks(1))
    }).await
}

async fn read_input(
    cx: &AsyncTaskContext,
    play_button: Entity,
    settings_button: Entity,
) -> Entity {
    // this could also be done with futures::select!
    let button_up_fut = cx.world_task(move |world| {
        [play_button, settings_button].observe_future::<ButtonUp>(world)
    }).await;
    button_up_fut.await.0
}
```
