# kafka-connect-directory-source
The connector is used to push file system events (additions and modifications of files in a directory) to Kafka.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration - Start with embedded DirWatcher
``` ini
name=directory-source
connector.class=org.apache.kafka.connect.directory.DirectorySourceConnector
tasks.max=1
directories.paths=./tmp1,./tmp2,./tmp3
check.dir.ms=1000
schema.name=directory_schema
topic=directory_topic
```