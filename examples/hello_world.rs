use bevy::prelude::*;
use bevy_mod_async::prelude::*;

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
struct Rotating {
    accumulated: f32,
}

#[derive(EntityEvent, Clone)]
struct FullRotation(Entity);

#[derive(Message, Clone)]
struct Text(&'static str);

#[derive(Message, Clone)]
struct InitMsg(&'static str);

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);

    let columns: usize = 10;
    let rows: usize = 10;
    let spacing = 50.0;
    let start_x = -225.0;
    let start_y = -225.0;

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
                    Rotating { accumulated: 0.0 },
                ))
                .id();

            if y == 0 && x == 0 {
                // Await single event.
                commands.spawn_task(async |cx| {
                    // Stream starts at creation time and may miss earlier events.
                    let full_rot_fut = cx.with_world(FullRotation::to_future).await;
                    let _ = full_rot_fut.await.unwrap();
                    println!("Some entity did a full rotation (Event)");
                });

                // Await event stream.
                commands.spawn_task(async |cx| {
                    // Stream starts at creation time and may miss earlier events.
                    let mut events = cx.with_world(FullRotation::event_stream).await;
                    let mut count = 0;
                    while events.next_event().await.is_ok() {
                        count += 1;
                        if count >= 5 {
                            println!("5 entities did a full rotation (EventStream)");
                            break;
                        }
                    }
                });

                // Await single entity event.
                commands.spawn_task(async move |cx| {
                    // Stream starts at creation time and may miss earlier events.
                    let full_rot_fut = cx
                        .with_world(move |w| entity.observe_future::<FullRotation>(w))
                        .await;
                    let e = full_rot_fut.await.unwrap();
                    println!("{} did a full rotation (EntityEvent)", e.0);
                });
            }

            // Await entity event stream.
            let mut toggle = (x + y).is_multiple_of(2);
            commands.spawn_task(async move |cx| {
                let color_a = Color::srgb(0.0, 1.0, 0.0);
                let color_b = Color::srgb(1.0, 0.0, 0.0);

                // Stream starts at creation time and may miss earlier events.
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

            // Await single message.
            if y == 0 && x < 5 {
                commands.spawn_task(async move |cx| {
                    let text = InitMsg::to_future(cx).await;
                    println!("{}: {}", entity, text.0);
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
            println!("{}", text.0);
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

        rot.accumulated += step;

        while rot.accumulated <= -std::f32::consts::TAU {
            rot.accumulated += std::f32::consts::TAU;
            commands.trigger(FullRotation(entity));
        }
    }
}
