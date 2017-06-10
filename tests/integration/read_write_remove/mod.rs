use std::env;
use std::ffi::CString;
use std::fs::File;
use std::io::Read;

use super::connect_to_cluster;


#[test]
fn read_write_remove() {
    let cluster = connect_to_cluster().unwrap();
    let pool = cluster.get_pool_context(c!("rbd")).unwrap();

    let mut test_blobs = Vec::new();

    let mut file_path = env::current_dir().unwrap();
    file_path.push("tests/integration/read_write_remove/test_file");

    for i in 0..5 {
        let file_name = format!("test_file{}", i);
        file_path.set_file_name(&file_name);
        println!("Reading file {}...", file_path.to_str().unwrap());

        let mut file = File::open(&file_path).unwrap();

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();

        println!("Writing to Ceph...");
        pool.write_full(CString::new(file_name).unwrap(), &bytes)
            .unwrap();

        println!("{} bytes written.", bytes.len());

        test_blobs.push(bytes);
    }

    for i in 0..5 {
        let file_name = CString::new(format!("test_file{}", i)).unwrap();

        println!("Reading test_file{} from Ceph...", i);

        let mut buf = Vec::new();
        pool.read_full(&*file_name, &mut buf).unwrap();

        println!("{} bytes read.", buf.len());

        assert!(test_blobs[i] == buf);

        pool.remove(file_name).unwrap();
    }

    for i in 0..5 {
        let file_name = CString::new(format!("test_file{}", i)).unwrap();

        let stat = pool.stat(file_name);

        assert!(stat.is_err());
    }
}
