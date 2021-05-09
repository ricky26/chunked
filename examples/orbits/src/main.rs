use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use glutin_window::GlutinWindow as Window;
use opengl_graphics::{GlGraphics, OpenGL};
use piston::event_loop::{Events, EventSettings};
use piston::input::{CloseEvent, RenderEvent};
use piston::window::WindowSettings;
use rand::Rng;
use rayon::prelude::*;
use tokio::time::{Duration, Instant, sleep_until};

use chunked::{BoxSystem, CommandBuffer, component, Component, EntityID, ModifySnapshot, System, SystemGroup, Universe, World};
use chunked::world::Lock;

const G: f32 = 6.67408e-11;
const TIME_STEP: f32 = 1.0;

#[derive(Debug, Clone, Copy, Default)]
pub struct Position(f32, f32, f32);
component!(Position);

#[derive(Debug, Clone, Copy, Default)]
pub struct Velocity(f32, f32, f32);
component!(Velocity);

#[derive(Debug, Clone, Copy, Default)]
pub struct Mass(f32);
component!(Mass);

pub struct ApplyNewtonianAccel(Arc<World>);

#[async_trait]
impl System for ApplyNewtonianAccel {
    async fn update(&mut self) {
        let previous_snapshot = self.0.snapshot().await;
        self.0.transaction(
            [
                Lock::Read(Position::type_id()),
                Lock::Write(Velocity::type_id()),
            ], move |mut tx| {
                tx.par_iter_chunks_mut().for_each(|chunk_a| {
                    let ids_a = chunk_a.components::<EntityID>().unwrap();
                    let positions_a = chunk_a.components::<Position>().unwrap();
                    let velocities_a = chunk_a.components_mut::<Velocity>().unwrap();

                    for idx_a in 0..ids_a.len() {
                        let id_a = ids_a[idx_a];
                        let Position(x_a, y_a, z_a) = positions_a[idx_a];
                        let Velocity(vx_a, vy_a, vz_a) = &mut velocities_a[idx_a];

                        for chunk_b in previous_snapshot.iter_chunks() {
                            let ids_b = chunk_b.components::<EntityID>().unwrap();
                            let positions_b = chunk_b.components::<Position>().unwrap();
                            let masses_b = chunk_b.components::<Mass>().unwrap();

                            for idx_b in 0..ids_b.len() {
                                let id_b = ids_b[idx_b];
                                let Position(x_b, y_b, z_b) = positions_b[idx_b];
                                let Mass(m_b) = masses_b[idx_b];

                                if id_b == id_a || m_b < 0.00001 {
                                    continue;
                                }

                                let dx = x_b - x_a;
                                let dy = y_b - y_a;
                                let dz = z_b - z_a;
                                let r2 = dx * dx + dy * dy + dz * dz;

                                // Bodies are overlapped!
                                if r2 < 0.0005 {
                                    continue;
                                }

                                let a = (G * m_b) / r2;
                                let r = r2.sqrt();
                                let vx = TIME_STEP * ((dx * a) / r);
                                let vy = TIME_STEP * ((dy * a) / r);
                                let vz = TIME_STEP * ((dz * a) / r);

                                *vx_a += vx;
                                *vy_a += vy;
                                *vz_a += vz;
                            }
                        }
                    }
                });
            },
        ).await;
    }
}

pub struct ApplyVelocity(Arc<World>);

#[async_trait]
impl System for ApplyVelocity {
    async fn update(&mut self) {
        self.0.transaction(
            [
                Lock::Read(Velocity::type_id()),
                Lock::Write(Position::type_id()),
            ], |mut tx| {
                tx.par_iter_chunks_mut().for_each(|chunk| {
                    let velocities = chunk.components::<Velocity>().unwrap();
                    let positions = chunk.components_mut::<Position>().unwrap();

                    for (Position(x, y, z), Velocity(vx, vy, vz))
                    in positions.iter_mut().zip(velocities.iter()) {
                        *x += TIME_STEP * vx;
                        *y += TIME_STEP * vy;
                        *z += TIME_STEP * vz;
                    }
                });
            },
        ).await;
    }
}

fn render_thread(world: Arc<World>) {
    const SCALE: f64 = 0.8;

    // Change this to OpenGL::V2_1 if not working.
    let opengl = OpenGL::V3_2;

    // Create an Glutin window.
    let mut window: Window = WindowSettings::new("orbits", [1024, 1024])
        .graphics_api(opengl)
        .exit_on_esc(true)
        .build()
        .unwrap();

    // Create a new game and run it.
    let mut gl = GlGraphics::new(opengl);

    let mut events = Events::new(EventSettings::new());
    while let Some(e) = events.next(&mut window) {
        if let Some(args) = e.render_args() {
            use graphics::*;

            const WHITE: [f32; 4] = [1.0, 1.0, 1.0, 1.0];
            const BLACK: [f32; 4] = [0.0, 0.0, 0.0, 1.0];

            let snap = block_on(world.snapshot());
            let square = rectangle::square(0.0, 0.0, 2.0);
            let (scale_x, scale_y) = (args.window_size[0] / 2.0, args.window_size[1] / 2.0);

            gl.draw(args.viewport(), |c, gl| {
                // Clear the screen.
                clear(BLACK, gl);

                // Render result!
                for chunk in snap.iter_chunks() {
                    let positions = chunk.components::<Position>().unwrap();

                    for Position(x, y, _) in positions.iter().copied() {
                        let x = ((x as f64) * scale_x * SCALE) + scale_x;
                        let y = ((y as f64) * scale_y * SCALE) + scale_y;

                        let transform = c
                            .transform
                            .trans(x, y);

                        rectangle(WHITE, square, transform, gl);
                    }
                }
            });
        }

        if e.close_args().is_some() {
            std::process::exit(0);
        }
    }
}

#[tokio::main]
async fn main() {
    let universe = Universe::new();
    let world = Arc::new(World::new(universe));

    // Populate universe!
    {
        const NUM_ENTITIES: usize = 1024;
        const VEL_SCALE: f32 = 0.005;

        let mut command_buffer = CommandBuffer::new();
        let mut rng = rand::thread_rng();

        for _ in 0..NUM_ENTITIES {
            let x = rng.gen::<f32>() * 2.0 - 1.0;
            let y = rng.gen::<f32>() * 2.0 - 1.0;
            let z = rng.gen::<f32>() * 2.0 - 1.0;

            let vx = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);
            let vy = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);
            let vz = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);

            let m = 1.0 + (50.0 * rng.gen::<f32>());

            let id = world.universe().allocate_entity();
            command_buffer.set_component(id, &Mass(m));
            command_buffer.set_component(id, &Position(x, y, z));
            command_buffer.set_component(id, &Velocity(vx, vy, vz));
        }

        world.exclusive_transaction(|snap| snap.modify(command_buffer.iter_edits())).await;
    }

    let update_world = world.clone();
    let update = async move {
        let mut system_set = SystemGroup::new();
        let accel_sys = system_set.insert(BoxSystem::new(ApplyNewtonianAccel(update_world.clone())));
        system_set.insert(BoxSystem::new(ApplyVelocity(update_world.clone()))
            .after(accel_sys));

        loop {
            let deadline = Instant::now() + Duration::from_millis(16);
            system_set.update().await;
            sleep_until(deadline).await;
        }
    };
    tokio::spawn(update);

    render_thread(world);
}
