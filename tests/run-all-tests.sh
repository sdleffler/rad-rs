#!/bin/bash

ceph_test_container=""

# iirc, Ceph does not play well with loopback - so, we grab the local
# address and use it for CEPH_PUBLIC_NETWORK/MON_IP.
#
# Get the local IP, using the route to the 8.8.8.8 DNS server to find the IP that
# a machine is *actually using.*
function get_local_ip() {
    ip -o route get 8.8.8.8 | sed -e 's/^.* src \([^ ]*\) .*$/\1/'
}

# Given an IP, "calculate" the subnet mask. We grep in case there are multiple
# valid ones.
function get_subnet_mask() {
    ip -o -f inet addr show | awk '/scope global/ {print $4}' | grep $1
}

# Start the ceph/demo docker container.
function start_docker() {
    local DOCKER0_SUBNET=`ip -o -f inet addr show | awk '/scope global docker0/ {print $4}'`

    echo "Building container..."

    local DOCKER_CONTAINER=`docker build ceph-test-docker | awk '/Successfully built/ { print $3 }'`

    docker ps

    # We store the running docker container's ID into the temporary file `.tmp_tc_name`
    # so that we can remember it through subshells and in case something goes wrong
    # and the docker container isn't stopped (i.e. Ctrl-C during running tests.)
    local DOCKER_CMD=""
    DOCKER_CMD+="docker run -d --rm --net=host -v $(pwd)/ceph:/etc/ceph "
    DOCKER_CMD+="-e CEPH_PUBLIC_NETWORK=${DOCKER0_SUBNET} "
    DOCKER_CMD+="-e MON_IP=127.0.0.1 "
    DOCKER_CMD+="--entrypoint=/preentry.sh ${DOCKER_CONTAINER}"

    $DOCKER_CMD > .tmp_tc_name
    
    echo "Started Ceph demo container: $(cat .tmp_tc_name)"
    echo "Waiting for Ceph demo container to be ready for tests..."

    docker ps

    ./do_until_success.sh "docker logs $(cat .tmp_tc_name) | grep -q '/entrypoint.sh: SUCCESS'" 2> /dev/null

    echo "Attempting to fix permissions on /etc/ceph/ceph/client.admin.keyring from inside the container..."

    # The devil's permissions for a total hack
    docker exec $(cat .tmp_tc_name) chmod 666 /etc/ceph/ceph.client.admin.keyring
    
    echo "Done."
    # if [[ ! -r ceph/ceph.client.admin.keyring || ! -w ceph/ceph.client.admin.keyring ]]; then
    #     echo "ceph/ceph.client.admin.keyring exists, but has incorrect permissions."
    #     echo "Attempting to run 'sudo chmod 644 ceph/ceph.client.admin.keyring' to fix this."
    #     sudo chmod 644 ceph/ceph.client.admin.keyring
    # fi
}

# Stop the last running ceph/demo docker container.
function stop_docker() {
    echo "Stopping docker container: $(docker kill $(cat .tmp_tc_name))"
}

# During setup, we kill the previous docker container if it's still running. Then,
# start a new one.
function setup() {
    (
        cd "$(dirname $0)"
        if [[ -e .tmp_tc_name && ! -z $(cat .tmp_tc_name) ]]; then
            echo "Previous docker container appears to still be running: $(cat .tmp_tc_name)"
            stop_docker
        fi

        start_docker
    )
}

# During teardown, we kill the running docker container and get rid of the temporary
# file recording the ID of the running docker container.
function teardown() {
    (
        cd "$(dirname $0)"
        stop_docker
        rm .tmp_tc_name
    )
}


export RUST_TEST_THREADS=1

setup || {
    teardown
    exit 1
}

echo "Successfully started Ceph demo container: $(cat .tmp_tc_name)"

cargo test --features integration-tests || {
    echo "In the case of a 'Permission denied' error connecting to the cluster,"
    echo "it may be necessary to run 'chmod 644 ceph/ceph.client.admin.keyring'."
    teardown
    exit 1
}

teardown
