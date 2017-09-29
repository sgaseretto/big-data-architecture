#!/bin/bash
function start(){
    docker-compose up
}

function restart(){
    docker-compose stop
    docker-compose start
}

function restart_hard(){
    docker-compose stop
    docker container rm bigdataarchitecture_cassandra_1
    docker container rm bigdataarchitecture_falcon_1
    docker container rm bigdataarchitecture_spark_1
    docker container rm bigdataarchitecture_kafka_1
    docker container rm bigdataarchitecture_zookeeper_1
    docker-compose up
}

function remove(){
    docker-compose stop
    docker container rm bigdataarchitecture_cassandra_1
    docker container rm bigdataarchitecture_falcon_1
    docker container rm bigdataarchitecture_spark_1
    docker container rm bigdataarchitecture_kafka_1
    docker container rm bigdataarchitecture_zookeeper_1
}

remove