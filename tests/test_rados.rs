#[cfg(feature = "integration-tests")]
#[macro_use]
extern crate rad;


#[cfg(feature = "integration-tests")]
mod integration {
    use std::env;
    use std::ffi::CString;

    use rad::{self, RadosConnectionBuilder, RadosCluster};


    fn connect_to_cluster() -> rad::Result<RadosCluster> {
        let mut ceph = env::current_dir().unwrap();
        ceph.push("tests");
        ceph.push("ceph");

        let mut ceph_conf = ceph.clone();
        ceph_conf.push("ceph.conf");

        let mut ceph_keyring = ceph;
        ceph_keyring.push("ceph.client.admin.keyring");

        RadosConnectionBuilder::with_user(c!("admin"))?
            .read_conf_file(CString::new(ceph_conf.to_str().unwrap()).unwrap())?
            .conf_set(c!("keyring"), CString::new(ceph_keyring.to_str().unwrap()).unwrap())?
            .connect()
    }


    mod connect;
    mod read_write_remove;
}
