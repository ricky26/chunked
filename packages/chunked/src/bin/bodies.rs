use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use rayon::iter::ParallelIterator;

use chunked::{BoxSystem, CommandBuffer, component, Component, EntityID, System, SystemGroup, Universe, World};
use chunked::world::Lock;

const G: f32 = 10.0;
const TIME_STEP: f32 = 1.0;

#[derive(Debug, Clone, Copy, Default)]
pub struct Position(f32, f32);
component!(Position);

#[derive(Debug, Clone, Copy, Default)]
pub struct Velocity(f32, f32);
component!(Velocity);

#[derive(Debug, Clone, Copy, Default)]
pub struct Mass(f32);
component!(Mass);

struct ApplyAcceleration(Arc<World>);

#[async_trait]
impl System for ApplyAcceleration {
    async fn update(&mut self) {
        let previous_snapshot = self.0.snapshot().await;
        self.0.transaction(
            [
                Lock::Read(Position::type_id()),
                Lock::Write(Velocity::type_id()),
                Lock::Write(Mass::type_id()),
            ], move |mut tx| {
                tx.par_iter_chunks_mut().for_each(|chunk_a| {
                    let ids_a = chunk_a.components::<EntityID>().unwrap();
                    let positions_a = chunk_a.components::<Position>().unwrap();
                    let velocities_a = chunk_a.components_mut::<Velocity>().unwrap();
                    let masses_a = chunk_a.components_mut::<Mass>().unwrap();

                    for idx_a in 0..ids_a.len() {
                        let id_a = ids_a[idx_a];
                        let Position(x_a, y_a) = positions_a[idx_a];
                        let Velocity(vx_a, vy_a) = &mut velocities_a[idx_a];
                        let Mass(m_a) = &mut masses_a[idx_a];

                        if *m_a <= 0.0 {
                            continue;
                        }

                        *vx_a = 0f32;
                        *vy_a = 0f32;

                        for chunk_b in previous_snapshot.iter_chunks() {
                            let ids_b = chunk_b.components::<EntityID>().unwrap();
                            let positions_b = chunk_b.components::<Position>().unwrap();
                            let masses_b = chunk_b.components::<Mass>().unwrap();

                            for idx_b in 0..ids_b.len() {
                                let id_b = ids_b[idx_b];

                                if id_b == id_a {
                                    continue;
                                }

                                let Position(x_b, y_b) = positions_b[idx_b];
                                let Mass(m_b) = masses_b[idx_b];

                                if m_b < 0.00001 {
                                    continue;
                                }

                                let dx = x_b - x_a;
                                let dy = y_b - y_a;
                                let r2 = dx * dx + dy * dy;

                                // Bodies are overlapped!
                                if r2 < 0.0005 {
                                    *m_a = 0.0;
                                    continue;
                                }

                                let a = (G * m_b) / r2;
                                let r = r2.sqrt();
                                let vx = TIME_STEP * ((dx * a) / r);
                                let vy = TIME_STEP * ((dy * a) / r);

                                *vx_a += vx;
                                *vy_a += vy;
                            }
                        }
                    }
                });
            },
        ).await;
    }
}

struct ApplyVelocity(Arc<World>);

#[async_trait]
impl System for ApplyVelocity {
    async fn update(&mut self) {
        self.0.transaction([
                               Lock::Read(Velocity::type_id()),
                               Lock::Write(Position::type_id()),
                           ], |mut tx| {
            tx.par_iter_chunks_mut().for_each(|chunk| {
                let velocities = chunk.components::<Velocity>().unwrap();
                let positions = chunk.components_mut::<Position>().unwrap();

                for (Position(x, y), Velocity(vx, vy))
                in positions.iter_mut().zip(velocities.iter()) {
                    *x += TIME_STEP * vx;
                    *y += TIME_STEP * vy;
                }
            });
        }).await;
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let universe = Universe::new();
    let world = Arc::new(World::new(universe));

    // Populate universe!
    {
        let mut command_buffer = CommandBuffer::new();

        const SQRT_NUM_ENTITIES: usize = 2;
        const SCALE: f32 = 2.0 / ((SQRT_NUM_ENTITIES - 1) as f32);

        for x in 0..SQRT_NUM_ENTITIES {
            for y in 0..SQRT_NUM_ENTITIES {
                let x = ((x as f32) * SCALE) - 1.0;
                let y = ((y as f32) * SCALE) - 1.0;

                let vx = y * 0.003;
                let vy = -x * 0.003;

                let id = world.universe().allocate_entity();
                command_buffer.set_component(id, &Mass(0.00001));
                command_buffer.set_component(id, &Position(x, y));
                command_buffer.set_component(id, &Velocity(vx, vy));
            }
        }

        block_on(world.exclusive_transaction(|snapshot| {
            snapshot.modify(command_buffer.iter_edits());
        }));
    }

    let mut systems = SystemGroup::new();

    let accel_sys = systems.insert(
        BoxSystem::new(ApplyAcceleration(world.clone())));
    systems.insert(
        BoxSystem::new(ApplyVelocity(world.clone()))
            .after(accel_sys));

    const SIZE: (i32, i32) = (500, 500);
    const OFFSET: (f32, f32) = (250.0, 250.0);
    const SCALE: (f32, f32) = (200.0, 200.0);
    const NUM_ITER: usize = 100;

    let mut dest = std::io::stdout();
    let mut last_positions = HashMap::new();

    write!(&mut dest, "<?xml version=\"1.0\" standalone=\"no\"?>\n")?;
    write!(&mut dest, "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.0//EN\" \"http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd\">\n")?;
    write!(&mut dest, "<svg xmlns=\"http://www.w3.org/2000/svg\" height=\"{}\" width=\"{}\">", SIZE.0, SIZE.1)?;
    write!(&mut dest, "<rect width=\"100%\" height=\"100%\" fill=\"black\"/>")?;

    for _ in 0..NUM_ITER {
        for _ in 0..10usize {
            block_on(systems.update());
        }

        // Render result!
        let snap = block_on(world.snapshot());
        for chunk in snap.iter_chunks() {
            let ids = chunk.components::<EntityID>().unwrap();
            let positions = chunk.components::<Position>().unwrap();
            let velocities = chunk.components::<Velocity>().unwrap();

            for (idx, id) in ids.iter().enumerate() {
                let Position(x, y) = positions[idx];
                let Velocity(vx, vy) = velocities[idx];
                let v2 = (vx * vx + vy * vy).sqrt();
                let a = 1.0 - (v2 / 0.03).max(0.0).min(0.7);

                if let Some((lx, ly)) = last_positions.get(id) {
                    let x1 = lx * SCALE.0 + OFFSET.0;
                    let y1 = ly * SCALE.1 + OFFSET.1;
                    let x2 = x * SCALE.0 + OFFSET.0;
                    let y2 = y * SCALE.1 + OFFSET.1;

                    write!(&mut dest,
                           "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" style=\"stroke:rgba(255,0,0,{});stroke-width:1\" />",
                           x1, y1, x2, y2, a)?;
                }

                last_positions.insert(*id, (x, y));
            }
        }
    }

    write!(&mut dest, "</svg>")?;
    Ok(())
}
