use std::env;
use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Write, BufRead, BufReader, BufWriter};

use super::connect_to_cluster;


fn writer() {
    let cluster = connect_to_cluster().unwrap();

    let pool = cluster.get_pool_context(c!("rbd")).unwrap();
    let mut obj_writer = BufWriter::new(pool.object(c!("test_file.obj")));

    let mut file_path = env::current_dir().unwrap();
    file_path.push("tests/integration/reader_writer/test_file.txt");
    let file = File::open(file_path).unwrap();
    let mut file_reader = BufReader::new(file);

    loop {
        let bytes_read = {
            let buf = file_reader.fill_buf().unwrap();

            if buf.len() == 0 {
                break;
            }

            obj_writer.write(buf).unwrap();

            buf.len()
        };

        file_reader.consume(bytes_read);
    }

    obj_writer.flush().unwrap();
}


fn reader() {
    let cluster = connect_to_cluster().unwrap();

    let pool = cluster.get_pool_context(c!("rbd")).unwrap();
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
