use super::connect_to_cluster;


#[test]
fn connect_to_ceph() {
    let mut cluster = connect_to_cluster().unwrap();

    let _stat = cluster.stat().unwrap();
}
