#[cfg(feature = "integration-tests")]
extern crate rad;


#[cfg(feature = "integration-tests")]
mod integration {
    use std::env;

    use rad::{self, RadosConnectionBuilder, RadosCluster};


    fn connect_to_cluster() -> rad::Result<RadosCluster> {
        let mut ceph = env::current_dir().unwrap();
        ceph.push("tests");
        ceph.push("ceph");

        let mut ceph_conf = ceph.clone();
        ceph_conf.push("ceph.conf");

        let mut ceph_keyring = ceph;
        ceph_keyring.push("ceph.client.admin.keyring");

        RadosConnectionBuilder::with_user("admin")?
            .read_conf_file(ceph_conf)?
            .conf_set("keyring", ceph_keyring.to_str().unwrap())?
            .connect()
    }


    mod connect;
    mod read_write_remove;
}
