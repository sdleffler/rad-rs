#[cfg(feature = "integration-tests")]
extern crate rad;

#[cfg(feature = "integration-tests")]
extern crate futures;

#[cfg(feature = "integration-tests")]
extern crate rand;


#[cfg(feature = "integration-tests")]
#[macro_use]
extern crate lazy_static;


#[cfg(feature = "integration-tests")]
mod integration {
    use std::env;
    use std::sync::Mutex;

    use rad::{self, RadosConnectionBuilder, RadosConnection};


    lazy_static! {
        static ref CLUSTER_HOLD: Mutex<()> = Mutex::new(());
    }


    fn connect_to_cluster() -> rad::Result<RadosConnection> {
        let ceph = env::current_dir().unwrap().join("tests/ceph");

        let ceph_conf = ceph.join("ceph.conf");
        let ceph_keyring = ceph.join("ceph.client.admin.keyring");

        RadosConnectionBuilder::with_user("admin")?
            .read_conf_file(&ceph_conf)?
            .conf_set("keyring", &ceph_keyring.to_string_lossy())?
            .connect()
    }


    mod connect;
    mod read_write_remove;
}
