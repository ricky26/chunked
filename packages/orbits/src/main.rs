use std::sync::Arc;
use glutin_window::GlutinWindow as Window;
use opengl_graphics::{GlGraphics, OpenGL};
use piston::event_loop::{EventSettings, Events};
use piston::input::{RenderEvent, CloseEvent};
use piston::window::WindowSettings;
use async_trait::async_trait;
use rand::Rng;
use rayon::prelude::*;
use ecs::{
    component,
    ComponentType,
    EntityID,
    World,
    CommandBuffer,
    System,
    SystemSet,
    SystemRegistration,
    SnapshotWriter,
};

const G: f32 = 6.67408e-11;
const TIME_STEP: f32 = 1.0;

#[derive(Debug,Clone,Copy,Default)]
pub struct Position(f32, f32, f32);
component!(Position);

#[derive(Debug,Clone,Copy,Default)]
pub struct Velocity(f32, f32, f32);
component!(Velocity);

#[derive(Debug,Clone,Copy,Default)]
pub struct Mass(f32);
component!(Mass);

pub struct ApplyNewtonianAccel;

#[async_trait]
impl System for ApplyNewtonianAccel {
    async fn update(&mut self, writer: &SnapshotWriter) {
        let writer: &'static SnapshotWriter = unsafe { std::mem::transmute(writer) };
        let snapshot = writer.snapshot();

        tokio::task::spawn_blocking(move || {
            (0..writer.num_chunks()).into_par_iter()
                .for_each(|chunk_index_a| {
                    let mut chunk_a = writer.borrow_chunk_mut(chunk_index_a);
                    let mut chunk_writer = chunk_a.writer();

                    let ids_a = chunk_writer.get_components::<EntityID>().unwrap();
                    let positions_a = chunk_writer.get_components::<Position>().unwrap();
                    let masses_a = chunk_writer.get_components_mut::<Mass>().unwrap();
                    let velocities_a = chunk_writer.get_components_mut::<Velocity>().unwrap();

                    ids_a.into_par_iter()
                        .zip(velocities_a.into_par_iter())
                        .zip(masses_a.into_par_iter())
                        .zip(positions_a.into_par_iter())
                        .for_each(|(((id_a, Velocity(ref mut vx_a, ref mut vy_a, ref mut vz_a)), Mass(ref mut m_a)), Position(x_a, y_a, z_a))| {        
                            if *m_a < 0.00001 {
                                return;
                            }
                            
                            for chunk_b in snapshot.iter_chunks() {
                                let ids_b = chunk_b.get_components::<EntityID>().unwrap();
                                let positions_b = chunk_b.get_components::<Position>().unwrap();
                                let masses_b = chunk_b.get_components::<Mass>().unwrap();

                                let (vxd, vyd, vzd) = ids_b.into_par_iter()
                                    .zip(masses_b.into_par_iter())
                                    .zip(positions_b.into_par_iter())
                                    .filter(|((id_b, Mass(m_b)), _)| (*id_b != id_a) && (*m_b >= 0.00001))
                                    .map(|((_, Mass(m_b)), Position(x_b, y_b, z_b))| {
                                        let dx = x_b - x_a;
                                        let dy = y_b - y_a;
                                        let dz = z_b - z_a;
                                        let r2 = dx * dx + dy * dy + dz * dz;
            
                                        // Bodies are overlapped!
                                        if r2 < 0.0005 {
                                            (0.0, 0.0, 0.0)
                                        } else {
                                            let a = (G * m_b) / r2;
                                            let r = r2.sqrt();
                                            let vx = TIME_STEP * ((dx * a) / r);
                                            let vy = TIME_STEP * ((dy * a) / r);
                                            let vz = TIME_STEP * ((dz * a) / r);
                                            (vx, vy, vz)
                                        }
                                    })
                                    .reduce(|| (0.0, 0.0, 0.0),
                                        |(a, b, c), (d, e, f)| (a + d, b + e, c + f));
                                    
                                *vx_a += vxd;
                                *vy_a += vyd;
                                *vz_a += vzd;
                            }
                        });
                });
        }).await.unwrap();
    }
}

pub struct ApplyVelocity;

#[async_trait]
impl System for ApplyVelocity {
    async fn update(&mut self, writer: &SnapshotWriter) {
        let component_types = &[
            ComponentType::for_type::<Velocity>(),
            ComponentType::for_type::<Position>(),
        ];
        let chunks = writer.iter_chunks_mut()
            .filter(|c| c.zone().archetype().has_all_component_types(component_types));

        for mut chunk in chunks {
            let mut chunk_writer = chunk.writer();
            let velocities = chunk_writer.get_components::<Velocity>().unwrap();
            let positions = chunk_writer.get_components_mut::<Position>().unwrap();

            for entity_idx in 0..positions.len() {
                let Velocity(vx, vy, vz) = velocities[entity_idx];
                let Position(ref mut x, ref mut y, ref mut z) = positions[entity_idx];
                
                *x = *x + TIME_STEP * vx;
                *y = *y + TIME_STEP * vy;
                *z = *z + TIME_STEP * vz;
            }
        }
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
    
            let snap = world.snapshot();
            let square = rectangle::square(0.0, 0.0, 2.0);
            let (scale_x, scale_y) = (args.window_size[0] / 2.0, args.window_size[1] / 2.0);
    
            gl.draw(args.viewport(), |c, gl| {
                // Clear the screen.
                clear(BLACK, gl);
                
                // Render result!
                for chunk in snap.iter_chunks() {
                    let positions = chunk.get_components::<Position>().unwrap();

                    for idx in 0..positions.len() {
                        let Position(x, y, _) = positions[idx];

                        let x = ((x as f64) * scale_x * SCALE) + scale_x;
                        let y = ((y as f64) * scale_y * SCALE) + scale_y;

                        let transform = c
                            .transform
                            .trans(x, y);
            
                        // Draw a box rotating around the middle of the screen.
                        rectangle(WHITE, square, transform, gl);
                    }
                }
            });
        }

        if let Some(_) = e.close_args() {
            std::process::exit(0);
        }
    }
}

#[tokio::main]
async fn main() {
    let world = Arc::new(World::new());
    let arch = world.ensure_archetype(vec![
        ComponentType::for_type::<Position>(),
        ComponentType::for_type::<Mass>(),
        ComponentType::for_type::<Velocity>(),
    ]);

    // Populate universe!
    {
        const NUM_ENTITIES: usize = 2048;
        const VEL_SCALE: f32 = 0.001;

        let mut command_buffer = CommandBuffer::new(world.clone());
        let mut rng = rand::thread_rng();

        for _ in 0..NUM_ENTITIES {
            let x = rng.gen::<f32>() * 2.0 - 1.0;
            let y = rng.gen::<f32>() * 2.0 - 1.0;
            let z = rng.gen::<f32>() * 2.0 - 1.0;
            
            let vx = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);
            let vy = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);
            let vz = VEL_SCALE * (rng.gen::<f32>() * 2.0 - 1.0);

            let m = 1.0 + (50.0 * rng.gen::<f32>());

            let id = command_buffer.new_entity(arch.clone());
            command_buffer.set_component(id, Mass(m));
            command_buffer.set_component(id, Position(x, y, z));
            command_buffer.set_component(id, Velocity(vx, vy, vz));
        }

        command_buffer.execute().await;
    }

    let render_world = world.clone();
    let _ = std::thread::spawn(move || render_thread(render_world));

    let mut system_set = SystemSet::new();
    let accel_sys = system_set.insert(SystemRegistration::from_system(ApplyNewtonianAccel)
        .read::<Position>()
        .read::<Mass>()
        .write::<Velocity>()).unwrap();
    system_set.insert(SystemRegistration::from_system(ApplyVelocity)
        .after(accel_sys)
        .read::<Velocity>()
        .write::<Position>()).unwrap();
    
    loop {
        system_set.update(&world).await;
        tokio::time::delay_for(std::time::Duration::from_millis(16)).await;
    }
}
