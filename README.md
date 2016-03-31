# kafka-connect-binary
The connector is used to load data binary file to Kafka from a Directory or a file path.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration - Start with embedded DirWatcher
``` ini
name=local-binary-source
connector.class=org.apache.kafka.connect.binary.BinarySourceConnector
tasks.max=1
tmp.path=./tmp
check.dir.ms=1000
schema.name=filebinaryschema
topic=file-binary
#filename.path= (not mandatory)
use.java.dirwatcher=true
```
## Start without embedded DirWatcher and with a file path
``` ini
name=local-binary-source
connector.class=org.apache.kafka.connect.binary.BinarySourceConnector
tasks.max=1
#tmp.path= (not mandatory)
#check.dir.ms= (not mandatory)
schema.name=filebinaryschema
topic=file-binary
filename.path=./path-to-your-file
use.java.dirwatcher=false
```

# Configuration for Producer, Consumer and Broker
``` ini
producer.max.request.size = 20000000  #max size for your file
consumer.fetch.message.max.bytes = 20000000
broker.replica.fetch.max.bytes = 20000000
broker.message.max.bytes = 20000000
```

