use std::io::Write;
use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use ecs::{
    component,
    ComponentType,
    World,
    CommandBuffer,
    ChunkSystem,
    SystemSet,
    SystemRegistration,
    Snapshot,
    SnapshotWriter,
    EntityID,
};

const G: f32 = 10.0;
const TIME_STEP: f32 = 1.0;

#[derive(Debug,Clone,Copy,Default)]
pub struct Position(f32, f32);
component!(Position);

#[derive(Debug,Clone,Copy,Default)]
pub struct Velocity(f32, f32);
component!(Velocity);

#[derive(Debug,Clone,Copy,Default)]
pub struct Mass(f32);
component!(Mass);

pub struct ApplyNewtonianAccel;

#[async_trait]
impl ChunkSystem for ApplyNewtonianAccel {
    async fn update(&mut self, snapshot: &Arc<Snapshot>, writer: &SnapshotWriter) {
        for chunk_index_a in 0..writer.num_chunks() {
            let mut chunk_a = writer.borrow_chunk_mut(chunk_index_a);
            let mut chunk_writer = chunk_a.writer();
            let positions_a = chunk_writer.get_components::<Position>().unwrap();
            let masses_a = chunk_writer.get_components_mut::<Mass>().unwrap();
            let velocities_a = chunk_writer.get_components_mut::<Velocity>().unwrap();

            for entity_a in 0..positions_a.len() {                
                let Mass(ref mut m_a) = masses_a[entity_a];
                let Position(x_a, y_a) = positions_a[entity_a];

                if *m_a < 0.00001 {
                    continue;
                }

                for (chunk_index_b, chunk_b) in snapshot.iter_chunks().enumerate() {
                    let positions_b = chunk_b.get_components::<Position>().unwrap();
                    let masses_b = chunk_b.get_components::<Mass>().unwrap();

                    for entity_b in 0..positions_b.len() {
                        if (chunk_index_a == chunk_index_b) && (entity_a == entity_b) {
                            continue;
                        }

                        let Mass(m_b) = masses_b[entity_b];
                        let Position(x_b, y_b) = positions_b[entity_b];
                        
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

                        velocities_a[entity_a].0 += vx;
                        velocities_a[entity_a].1 += vy;
                    }
                }
            }
        }
    }
}

pub struct ApplyVelocity;

#[async_trait]
impl ChunkSystem for ApplyVelocity {
    async fn update(&mut self, _snapshot: &Arc<Snapshot>, writer: &SnapshotWriter) {
        for chunk_index in 0..writer.num_chunks() {
            let mut chunk_a = writer.borrow_chunk_mut(chunk_index);
            let mut chunk_writer = chunk_a.writer();
            let velocities = chunk_writer.get_components::<Velocity>().unwrap();
            let positions = chunk_writer.get_components_mut::<Position>().unwrap();

            for entity_idx in 0..positions.len() {
                let Velocity(vx, vy) = velocities[entity_idx];
                let Position(ref mut x, ref mut y) = positions[entity_idx];
                
                *x = *x + TIME_STEP * vx;
                *y = *y + TIME_STEP * vy;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let world = Arc::new(World::new());
    let arch = world.ensure_archetype(vec![
        ComponentType::for_type::<Position>(),
        ComponentType::for_type::<Mass>(),
        ComponentType::for_type::<Velocity>(),
    ]);

    // Populate universe!
    {
        let mut command_buffer = CommandBuffer::new(world.clone());

        const SQRT_NUM_ENTITIES: usize = 2;
        const SCALE: f32 = 2.0 / ((SQRT_NUM_ENTITIES - 1) as f32);

        for x in 0..SQRT_NUM_ENTITIES {
            for y in 0..SQRT_NUM_ENTITIES {
                let x = ((x as f32) * SCALE) - 1.0;
                let y = ((y as f32) * SCALE) - 1.0;

                let vx = y * 0.003;
                let vy = -x * 0.003;

                let id = command_buffer.new_entity(arch.clone());
                command_buffer.set_component(id, Mass(0.00001));
                command_buffer.set_component(id, Position(x, y));
                command_buffer.set_component(id, Velocity(vx, vy));
            }
        }

        command_buffer.execute().await;
    }

    let mut system_set = SystemSet::new();

    let accel_sys = system_set.insert(SystemRegistration::new_chunk(ApplyNewtonianAccel)
        .read::<Position>()
        .read::<Mass>()
        .write::<Velocity>()).unwrap();
    system_set.insert(SystemRegistration::new_chunk(ApplyVelocity)
        .after(accel_sys)
        .read::<Velocity>()
        .write::<Position>()).unwrap();
    
    const SIZE: (i32, i32) = (500, 500);
    const OFFSET: (f32, f32) = (250.0, 250.0);
    const SCALE: (f32, f32) = (200.0, 200.0);
    const NUM_ITER: usize = 100;

    let mut dest = std::io::stdout();
    let mut last_positions = HashMap::new();

    write!(&mut dest, "<?xml version=\"1.0\" standalone=\"no\"?>\n")?;
    write!(&mut dest, "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.0//EN\" \"http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd\">\n")?;
    write!(&mut dest, "<svg xmlns=\"http://www.w3.org/2000/svg\" height=\"{}\" width=\"{}\">", SIZE.0, SIZE.1)?;

    for _ in 0..NUM_ITER {
        for _ in 0..10usize {
            system_set.update(&world).await;
        }

        // Render result!
        let snap = world.snapshot();
        for chunk in snap.iter_chunks() {
            let ids = chunk.get_components::<EntityID>().unwrap();
            let positions = chunk.get_components::<Position>().unwrap();
            let velocities = chunk.get_components::<Velocity>().unwrap();

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
