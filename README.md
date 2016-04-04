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
tasks.max=1 # not really needed
connector.class=org.apache.kafka.connect.directory.DirectorySourceConnector
directories.paths=./tmp1,./tmp2,./tmp3
check.dir.ms=1000 # optional, default is 1000
schema.name=directory_schema
topic=directory_topic
```

or as a JSON

```json
{
	"name": "directory-source",
	"tasks.max": 1,
	"connector.class": "org.apache.kafka.connect.directory.DirectorySourceConnector",
	"schema.name": "directory_schema",
	"topic": "directory_topic",
	"check.dir.ms": 1000,
	"directories.paths": "/path/to/dir/1,/path/to/dir/2,/path/to/dir/3",
}
```

- **name**: name of the connector
- **connector.class**: class of the implementation of the connector
- **schema.name**: name to use for the schema
- **topic**: name of the topic to append to
- **directories.paths**: comma separated list of directories to watch, one per task
- **check.dir.ms**: interval at which to check for updates in the directories