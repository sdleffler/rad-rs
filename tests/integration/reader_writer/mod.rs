use std::env;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read, Write, BufRead, BufReader, BufWriter};

use super::connect_to_cluster;


fn writer() {
    let mut cluster = connect_to_cluster().unwrap();

    let mut pool = cluster.get_pool_context(c!("rbd")).unwrap();
    let mut object = pool.object(c!("test_file.obj"));

    let mut file_path = env::current_dir().unwrap();
    file_path.push("tests/integration/reader_writer/test_file.txt");
    let mut file = File::open(file_path).unwrap();

    io::copy(&mut file, &mut object);

    object.flush().unwrap();
}


fn reader() {
    let mut cluster = connect_to_cluster().unwrap();

    let mut pool = cluster.get_pool_context(c!("rbd")).unwrap();
    let obj_reader = BufReader::new(pool.object(c!("test_file.obj")));

    let mut file_path = env::current_dir().unwrap();
    file_path.push("tests/integration/reader_writer/test_file.txt");
    let file = File::open(file_path).unwrap();
    let file_reader = BufReader::new(file);

    assert!(obj_reader.bytes().map(Result::unwrap).eq(file_reader.bytes().map(Result::unwrap)));
}


#[test]
fn reader_writer() {
    writer();
    reader();
}
