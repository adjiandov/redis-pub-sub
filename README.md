# Pub/Sub Solutions

This is a sample implementation of the problem statement for implementing consumer groups for Redis pub/sub channel.

## Installation

In order to make the solution run you need several things to prepare.
1. Install Redis (server) from https://redis.io
2. Install Python(3) -> https://www.python.org/downloads/
4. Requires Java 21 (If this is not available, needs to be changed in [build.gradle.kts](build.gradle.kts))

## Run the solution
1. Install Redis (The solution assumes that it will run on the default port - 6379)
2. Run the following commands on the root directory of this repo: 
   1. ```./gradlew clean bootJar```
   2. ```java -jar .build/libs/redis-demo-0.0.1-SNAPSHOT.jar```
3. Run the following python script:
```python
import random
from datetime import datetime, timedelta
import time
import uuid
import redis
# Redis connection details (modify host and port if needed)
redis_host = "localhost"
redis_port = 6379
target_duration = timedelta(minutes=1)
batch_size = 1000

def publisher():
    try:
        connection = redis.Redis(host=redis_host, port=redis_port)
    except redis.ConnectionError:
        print("Error: Failed to connect to Redis server")
        exit(1)

    start_time = datetime.now()
    total_messages = 0
    try:
        while datetime.now() - start_time < target_duration:
            p = connection.pipeline()
            for _ in range(batch_size):
                p.publish(
                    "messages:published", f'{{"message_id": "{str(uuid.uuid4())}"}}'
                )
            p.execute()
            total_messages += batch_size
            time.sleep(random.uniform(0.1, 0.5))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print(f"Total messages published: {total_messages}")

if __name__ == "__main__":
    publisher()
```


## Observe

Observe the logs for the reported metrics.

## Configuration
In [application.properties](src/main/resources/application.properties) you can configure the following properties:
```redis.pubsub.consumer-count``` - To configure the number of consumers
```message.processing.reporting.rate.ms``` - The reporting rate in milliseconds (defaults to 3000)

## Testing notes
In order for the integration test to run, it requires to have a running docker environment on the machine.

## Future improvements

1. Even though the current solution "simulates" distribution on one JVM, the solution is ready to support actual distributed consumers on separate instances, since each consumer is acquiring the right to process through a Redis Lock.
2. The more natural way to implement the desired solution is replacing the Redis pub/sub with Redis stream. Redis streams support consumer groups by default and take care about the requirement that only one consumer processes the next message. They even support keeping the messages if there is no consumer subscribed by the time messages are added to the stream.
