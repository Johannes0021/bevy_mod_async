use bevy::prelude::*;
use bevy_mod_async::prelude::*;
use std::time::Duration;

fn main() {
    App::new()
        .add_plugins((DefaultPlugins, AsyncTaskPlugin))
        .add_message::<Text>()
        .add_message::<InitMsg>()
        .add_systems(Startup, setup)
        .add_systems(Update, rotate_quad)
        .run();
}

#[derive(Component)]
struct Rotating(f32);

#[derive(EntityEvent, Clone)]
struct FullRotation(Entity);

#[derive(Message, Clone)]
struct Text(String);

#[derive(Message, Clone)]
struct InitMsg(&'static str);

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);

    // World access
    commands.spawn_async_task(async |cx| {
        // once
        let entity_count = cx.world_task_once(|world| world.entity_count()).await;
        info!("World contains {entity_count} entities.");

        // looped + detach
        cx.world_task_looped(|world| {
            let entity_count = world.entity_count();
            info!("World contains {entity_count} entities.");
            WorldTaskLoop::<()>::Continue(WorldTaskDelay::secs(1.0)) // Delay secs
        })
        .detach(); // Detach means: Start the task, we don't want to wait for it.

        // delay + schedule
        let ticks = 1024;
        let entity_count = cx
            .world_task_once(|world| world.entity_count())
            .delay(WorldTaskDelay::Ticks(ticks)) // Delay ticks
            .schedule(WorldTaskSchedule::FixedUpdate) // Schedule
            .await;
        info!("World contains {entity_count} entities after {ticks} fixed update ticks.");

        // spawn
        // Spawn means: Start the task and give us the receiver.
        let entity_count = cx.world_task_once(|world| world.entity_count()).spawn();
        info!("World contains {} entities.", entity_count.await);

        // delay only
        let secs = 2.1;
        cx.delay(WorldTaskDelay::Elapsed(Duration::from_secs_f64(secs)))
            .await;
        info!("Delay: {secs}s elapsed (update)");

        let secs = 2.1;
        cx.delay(WorldTaskDelay::Elapsed(Duration::from_secs_f64(secs)))
            .schedule(WorldTaskSchedule::FixedUpdate) // Schedule
            .await;
        info!("Delay: {secs}s elapsed (fixed update)");
    });

    // Await single event.
    commands.spawn_async_task(async |cx| {
        // The stream starts at creation time and misses earlier events.
        let full_rot_fut = cx.world_task_once(FullRotation::to_future).await;
        let _: FullRotation = full_rot_fut.await;
        info!("Some entity did a full rotation (Event)");
    });

    let columns: usize = 10;
    let rows: usize = 10;
    let spacing = 50.0;
    let start_x = -225.0;
    let start_y = -225.0;

    // Await event stream.
    commands.spawn_async_task(async move |cx| {
        // The stream starts at creation time and misses earlier events.
        let mut events = cx.world_task_once(FullRotation::event_stream).await;
        let amount = rows * columns * 2;
        for _ in 0..amount {
            let _: FullRotation = events.next_event().await;
        }
        info!("Received {} FullRotation events (EventStream)", amount);
    });

    for y in 0..rows {
        for x in 0..columns {
            let entity = commands
                .spawn((
                    Sprite {
                        color: Color::srgb(0.0, 0.0, 1.0),
                        custom_size: Some(Vec2::new(40.0, 40.0)),
                        ..default()
                    },
                    Transform::from_xyz(
                        start_x + (x as f32) * spacing,
                        start_y + (y as f32) * spacing,
                        0.0,
                    ),
                    Rotating(0.0),
                ))
                .id();

            if y == 0 && x == 0 {
                // Await single entity event.
                commands.spawn_async_task(async move |cx| {
                    // The stream starts at creation time and misses earlier events.
                    let full_rot_fut = cx
                        .world_task_once(move |w| entity.observe_future::<FullRotation>(w))
                        //.world_task_once(move |w| [entity, ...].observe_future::<FullRotation>(w))
                        .await;
                    let e: FullRotation = full_rot_fut.await;
                    info!("{} did a full rotation (EntityEvent)", e.0);
                });
            }

            // Await entity event stream.
            let mut toggle = (x + y).is_multiple_of(2);
            commands.spawn_async_task(async move |cx| {
                let color_a = Color::srgb(0.0, 1.0, 0.0);
                let color_b = Color::srgb(1.0, 0.0, 0.0);

                // The stream starts at creation time and misses earlier events.
                let mut events = cx
                    .world_task_once(move |w| entity.event_stream::<FullRotation>(w))
                    //.world_task_once(move |w| [entity, ...].event_stream::<FullRotation>(w))
                    .await;
                loop {
                    let _: FullRotation = events.next_event().await;

                    let next_color = if toggle { color_a } else { color_b };
                    toggle = !toggle;

                    cx.world_task_once(move |w| {
                        let mut entity = w.entity_mut(entity);
                        let mut sprite = entity.get_mut::<Sprite>().unwrap();
                        sprite.color = next_color;
                    })
                    .await;
                }
            });

            if y == 0 && x < 5 {
                // Await single message.
                commands.spawn_async_task(async move |cx| {
                    // The stream starts at creation time and may miss earlier messages.
                    let text_fut = cx.world_task_once(|w| InitMsg::to_future(w)).await;
                    let text = text_fut.await;
                    info!("{}: {}", entity, text.0);
                });
            }
        }
    }

    commands.write_message(InitMsg("Hello!"));

    // Await message stream.
    commands.spawn_async_task(async |cx| {
        // The stream starts at creation time and may miss earlier messages.
        let mut messages = cx.world_task_once(|w| Text::message_stream(w)).await;

        let mut msg_count = 0;
        cx.world_task_looped(move |world| {
            msg_count += 1;
            world.write_message(Text(format!("Message {msg_count}")));

            if msg_count >= 21 {
                return WorldTaskLoop::Return(());
            }

            WorldTaskLoop::Continue(WorldTaskDelay::secs(2.1))
        })
        .detach();

        loop {
            let text = messages.next_message().await;
            info!("{}", text.0);
        }
    });
}

fn rotate_quad(
    time: Res<Time>,
    mut q: Query<(Entity, &mut Transform, &mut Rotating)>,
    mut commands: Commands,
) {
    let step = -2.0 * time.delta_secs();

    for (entity, mut t, mut rot) in &mut q {
        t.rotate_z(step);
        rot.0 += step;
        while rot.0 <= -std::f32::consts::TAU {
            rot.0 += std::f32::consts::TAU;
            commands.trigger(FullRotation(entity));
        }
    }
}
