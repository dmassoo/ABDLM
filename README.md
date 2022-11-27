# ABDLM-GEN
## Setup
- install docker desktop https://docs.docker.com/desktop/install/windows-install/
- go to root dir of this project
- run docker compose in daemon
```
docker compose up -d
```
building containers will take some time
If one of custom containers fail - restart it in docker

use the following command to stop containers:
```
docker compose down
```

To see kafka stats go to http://localhost:8081/ in your brouser
To see spark stats go to http://localhost:8080/ in your brouser
## Modifications

To add code you should put it to corresponding _code_ folder. This folder is
mounted to container. Also container can be attached using VScode with
appropriate extensions.

## Description

- __my-cassandra-1__ - container with cassandra server

- __my-ui-1__ - container with kafka stats ui

- __my-kafka-1__ - container with kafka broker (kraft mode)

- __my-client-1__ - container with kafka consumer (reads from kafka and writes to cassandra)

- __my-generator-1__ - generates data and puts it to kafka

- __my-spark-master__ - master spark node

- __my-spark-worker__ - worker spark node

- __my_spark-client__ - client that reads from kafka via spark interface




