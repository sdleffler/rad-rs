#!/bin/bash

if [[ -e /etc/ceph ]]; then 
    echo "/etc/ceph exists - removing contents..."

    ls -l /etc/ceph
    rm -rf /etc/ceph/*
fi

echo "Starting Ceph..."

/entrypoint.sh
