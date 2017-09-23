use super::{CLUSTER_HOLD, connect_to_cluster};


#[test]
fn connect_to_ceph() {
    let lock = CLUSTER_HOLD.lock().unwrap();

    let mut cluster = connect_to_cluster().unwrap();

    let _stat = cluster.stat().unwrap();

    let _ = lock;
}
