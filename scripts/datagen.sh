#! /bin/bash

set -x

VERSION=0.1.0-alpha

function clean {
    mvn clean
}

function build {
    mvn package -DskipTests
}

function gen {
    python3 scripts/run.py --jar target/ldbc_finbench_datagen-${VERSION}-jar-with-dependencies.jar --parallelism 1 -- --format csv --scale-factor 0.1
}

$1
