use futures::prelude::*;
use futures::stream;
use rand::{Rng, SeedableRng, XorShiftRng};

use rad::errors::*;

use super::{CLUSTER_HOLD, connect_to_cluster};


const NUM_OBJECTS: usize = 64;


#[test]
fn read_write_remove() {
    let lock = CLUSTER_HOLD.lock().unwrap();

    let mut cluster = connect_to_cluster().unwrap();

    let bytes: Vec<_> = (0..NUM_OBJECTS)
        .map(|i| {
            let mut buf = Vec::new();

            buf.extend(
                XorShiftRng::from_seed([i as u32 + 1, 2, 3, 4])
                    .gen_iter::<u8>()
                    .take(1 << 16),
            );

            buf
        })
        .collect();

    let names: Vec<_> = (0..NUM_OBJECTS).map(|i| format!("obj-{}", i)).collect();

    (0..NUM_OBJECTS)
        .map(|i| -> Result<()> {
            let mut pool = cluster.get_pool_context("rbd").unwrap();
            let name = &names[i];
            let data = &bytes[i];

            println!("Beginning write for {}", name);
            pool.write_full(name, data)?;
            println!("Finished write for {}, asserting existence", name);
            assert!(pool.exists(name)?);
            println!("Existence of {} asserted, beginning data check", name);
            let mut buf = vec![0u8; data.len()];
            let n = pool.read(name, &mut buf, 0)?;
            assert!(n == buf.len());
            assert!(&buf == data);
            println!("Data equality asserted, beginning removal of {}", name);
            pool.remove(name)?;
            println!("Finished removal, asserting inexistence of {}", name);
            assert!(!pool.exists(name)?);
            println!("Asserted inexistence of {}", name);

            Ok(())
        })
        .for_each(|result| result.unwrap());

    let _ = lock;
}


#[test]
fn read_write_remove_async() {
    let lock = CLUSTER_HOLD.lock().unwrap();

    let mut cluster = connect_to_cluster().unwrap();

    let bytes: Vec<_> = (0..NUM_OBJECTS)
        .map(|i| {
            let mut buf = Vec::new();

            buf.extend(
                XorShiftRng::from_seed([i as u32 + 1, 2, 3, 4])
                    .gen_iter::<u8>()
                    .take(1 << 16),
            );

            buf
        })
        .collect();

    let names: Vec<_> = (0..NUM_OBJECTS).map(|i| format!("obj-{}", i)).collect();

    let writes = (0..NUM_OBJECTS).map(|i| {
        let mut pool = cluster.get_pool_context("rbd").unwrap();
        let name = &names[i];
        let data = &bytes[i];

        println!("Beginning write for {}", name);
        pool.write_full_async(name, data)
                .and_then(move |()| {
                    println!("Finished write for {}, beginning existence check", name);
                    pool.exists_async(name)
                        .and_then(move |b| {
                            assert!(b);
                            println!("Existence of {} asserted, beginning data check", name);
                            pool.read_async(name, vec![0u8; data.len()], 0)
                                .and_then(move |buf| {
                                    assert!(&buf == data);
                                    println!("Data equality asserted, beginning removal of {}", name);
                                    pool.remove_async(name).and_then(move |()| {
                                        println!("Finished removal, asserting inexistence of {}", name);
                                        pool.exists_async(name).map(move |b| {
                                            assert!(!b);
                                            println!("Asserted inexistence of {}", name);
                                        })
                                    })
                                })
                        })
                })
    });

    stream::iter_ok(writes)
        .buffered(NUM_OBJECTS)
        .collect()
        .wait()
        .unwrap();

    let _ = lock;
}
