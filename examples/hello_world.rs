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
struct Text(&'static str);

#[derive(Message, Clone)]
struct InitMsg(&'static str);

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);

    // World access
    commands.spawn_task(async |cx| {
        let entity_count = cx.with_world(|world| world.entity_count()).await;
        info!("World contains {entity_count} entities.");
    });
    commands.spawn_task(async |cx| {
        let ticks = 128;
        let entity_count = cx
            .with_world_scheduled(RunAfter::UpdateTicks(ticks), |world| world.entity_count())
            .await;
        info!("World contains {entity_count} entities after {ticks} update ticks.");
    });
    commands.spawn_task(async |cx| {
        let ticks = 256;
        let entity_count = cx
            .with_world_scheduled(RunAfter::FixedUpdateTicks(ticks), |world| {
                world.entity_count()
            })
            .await;
        info!("World contains {entity_count} entities after {ticks} fixed update ticks.");
    });
    commands.spawn_task(async |cx| {
        let secs = 1;
        let entity_count = cx
            .with_world_scheduled(
                RunAfter::UpdateElapsed(Duration::from_secs(secs)),
                |world| world.entity_count(),
            )
            .await;
        info!("World contains {entity_count} entities after {secs}s elapsed. (update)");
    });
    commands.spawn_task(async |cx| {
        let secs = 3;
        let entity_count = cx
            .with_world_scheduled(
                RunAfter::FixedUpdateElapsed(Duration::from_secs(secs)),
                |world| world.entity_count(),
            )
            .await;
        info!("World contains {entity_count} entities after {secs}s elapsed. (fixed update)");
    });
    commands.spawn_task(async |cx| {
        let secs = 7.21;
        let entity_count = cx
            .with_world_scheduled(RunAfter::UpdateElapsedSecs(secs), |world| {
                world.entity_count()
            })
            .await;
        info!("World contains {entity_count} entities after {secs}s elapsed. (update)");
    });
    commands.spawn_task(async |cx| {
        let secs = 10.21;
        let entity_count = cx
            .with_world_scheduled(RunAfter::FixedUpdateElapsedSecs(secs), |world| {
                world.entity_count()
            })
            .await;
        info!("World contains {entity_count} entities after {secs}s elapsed. (fixed update)");
    });

    // Delay
    commands.spawn_task(async |cx| {
        let ticks = 21;
        let secs = 1.21;
        let duration = Duration::from_secs_f64(secs);

        cx.delay(RunAfter::UpdateTicks(ticks)).await;
        info!("Delay: {ticks} ticks (update)");

        cx.delay(RunAfter::FixedUpdateTicks(ticks)).await;
        info!("Delay: {ticks} ticks (fixed update)");

        cx.delay(RunAfter::UpdateElapsed(duration)).await;
        info!("Delay: {secs}s elapsed (update Duration)");

        cx.delay(RunAfter::FixedUpdateElapsed(duration)).await;
        info!("Delay: {secs}s elapsed (fixed update Duration)");

        cx.delay(RunAfter::UpdateElapsedSecs(secs)).await;
        info!("Delay: {secs}s elapsed (update f64)");

        cx.delay(RunAfter::FixedUpdateElapsedSecs(secs)).await;
        info!("Delay: {secs}s elapsed (fixed update f64)");
    });

    // Await single event.
    commands.spawn_task(async |cx| {
        // The stream starts at creation time and misses earlier events.
        let full_rot_fut = cx.with_world(FullRotation::to_future).await;
        let _ = full_rot_fut.await.unwrap();
        info!("Some entity did a full rotation (Event)");
    });

    let columns: usize = 10;
    let rows: usize = 10;
    let spacing = 50.0;
    let start_x = -225.0;
    let start_y = -225.0;

    // Await event stream.
    commands.spawn_task(async move |cx| {
        // The stream starts at creation time and misses earlier events.
        let mut events = cx.with_world(FullRotation::event_stream).await;
        let amount = rows * columns * 2;
        for _ in 0..amount {
            events.next_event().await.unwrap();
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
                commands.spawn_task(async move |cx| {
                    // The stream starts at creation time and misses earlier events.
                    let full_rot_fut = cx
                        .with_world(move |w| entity.observe_future::<FullRotation>(w))
                        .await;
                    let e = full_rot_fut.await.unwrap();
                    info!("{} did a full rotation (EntityEvent)", e.0);
                });
            }

            // Await entity event stream.
            let mut toggle = (x + y).is_multiple_of(2);
            commands.spawn_task(async move |cx| {
                let color_a = Color::srgb(0.0, 1.0, 0.0);
                let color_b = Color::srgb(1.0, 0.0, 0.0);

                // The stream starts at creation time and misses earlier events.
                let mut events = cx
                    .with_world(move |w| entity.event_stream::<FullRotation>(w))
                    .await;
                while events.next_event().await.is_ok() {
                    let next_color = if toggle { color_a } else { color_b };
                    toggle = !toggle;

                    cx.with_world(move |w| {
                        let mut entity = w.entity_mut(entity);
                        let mut sprite = entity.get_mut::<Sprite>().unwrap();
                        sprite.color = next_color;
                    })
                    .await;
                }
            });

            if y == 0 && x < 5 {
                // Await single message.
                commands.spawn_task(async move |cx| {
                    let text = InitMsg::to_future(cx).await;
                    info!("{}: {}", entity, text.0);
                });
            }
        }
    }

    commands.write_message(InitMsg("Hello!"));

    // Await message stream.
    commands.write_message(Text("Message 1"));
    commands.write_message(Text("Message 2"));

    commands.spawn_task(async |cx| {
        let mut messages = Text::message_stream(cx);
        loop {
            let text = messages.next_message().await;
            info!("{}", text.0);
        }
    });

    commands.write_message(Text("Message 3"));
    commands.write_message(Text("Message 4"));
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
