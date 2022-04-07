# doodle-challenge

From [Doodle's repository](https://github.com/DoodleScheduling/hiring-challenges/tree/master/data-engineer)

I've been doing data analysis for basically my entire career, so I'm very comfortable with Python and that's what I'll be using. I'm using a MacBook Pro, and to save time I'll run everything locally (instead of running a Docker container for instance). I used an ipython notebook for prototyping and having the output saved for the reviewer.

# 1. Install Kafka

After installing Kafka with `brew` (on MacOS), I fought a little with some configuration and managed to start a server using

```
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

kafka-server-start /usr/local/etc/kafka/server.properties
```

# 2. Create a topic

After launch, I created a new topic using

```
kafka-topics --bootstrap-server localhost:9092 --create --topic stream
```

and verified the creation with

```
kafka-topics --list --bootstrap-server localhost:9092
```

# 3. Send data

The simplest way of doing this was to use the provided snippet and the example file

```
gzcat stream.gz | kafka-console-producer --broker-list localhost:9092 --topic stream
```

The problem here is that it wasn't easy to simulate a real stream with late timestamps.

# 4. Send data to stdout

The obvious way is to use the command line

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic stream --from-beginning | head -n 1
```

but of course the output is not really helpful. 

**But Python is here to help!** For this I created an iPython notebook to test various strategies (omitting the setup) and leaving the output saved. Find it here [doodle_challenge.ipynb](doodle_challenge.ipynb). Further steps will also use this notebook. I didn't put the result in a .py file just to keep things tidier.

# 5. Simple count

I tried different ways of counting. Firstly, I created a dictionary, where keys would be minutes, and values would be:
- Using a list and then using `set()` over it when counting
- Using collections.Counter
- Using a set directly

Since performance was basically the same in all cases (as measured with `%time`), I stuck with the latter to save some memory. Then, I dropped the dictionary structure and kept only the current minute, which I would print to stdout as soon as the next minute started. **Note** that this only works because the original stream is ordered (checked it in the notebook) and no timestamps are late.

## Strategy for late timestamps and unordered set

The strategy for late timestamps could be one of the following:
- If we know that 99.9% of messages arrive within 5 seconds, and we don't care about the 0.1% of the messages that arrive afterwards, we can get by just by adding a `previous_minute` and `previous_minute_users` in `count_and_print_asap` function, and print the previous minute when the current time is, say, 5 seconds ahead of it.
- Another strategy can be to aggregate overlapping minutes, for instance: `"15:39:00-15:40:00", "15:39:30-15:40:30", ...
- We could publish ASAP, as I have done, but keep counting for a given minute and publish `missed_users` at a given point.

For bitflips the story would have been a bit different. I guess we could define a range around the current timestamp, `ct ± X seconds` and check if the timestamp of the current message is within the window, and reject it otherwise. Also, we could think of a simple function that tries to ajust the timestamp starting from the biggest error (i.e. left to right in a timestamp represented in unix seconds) and minimize the error with repect to the current timestamp. For example, if the maximum error I allow is 5 seconds, I would only need to check the last digit. I left a draft at the bottom of the notebook about this. 

# 6. Benchmark

I never did any benchmarking in the past. I looked a bit around and found a [memory_profiler](https://github.com/pythonprofilers/memory_profiler) library. It offers quite a bit of stuff, but for the sake of time I sticked to the basic `%memit` ipython magic to check memory consumption of the different functions and, as mentioned in step 5, `%time` for measuring wall time. `count_and_print_asap` function also has some manually crafted timer to see incremental times and computing metrics like users/second.

# 7. Output to a new Kafka Topic

First of all, I created a new topic called `processed` and tried to send a message by hand (as prompted in bonus section).

Using 
```
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

producer.send('processed', {'test-key': 'test-value'})
```

I tested this manually in the terminal via
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic processed --from-beginning

# Output
{"test-key": "test-value"}
Processed a total of 1 messages
```

Then, I created a new function `count_and_publish_asap`, which publishes JSON values of the type `{minute: user_count}` as soon as a minute is over. Once again, we could implement a delay (or think about other strategies as mentioned above).


# 8. Measure performance and optimize

I kind of did this already in step 6. The difference is that now, when I output to a Kafka producer, I don't need to keep track of timestamps anymore, and can measure metrics directly on that.

# 9. How can we scale

Different partitioning strategies can definitely help (for example one partition per minute?). This way the consumer can operate in parallel and continue monitoring for late timestamps, since we would know they'd come from a given partition, in order.

Reducing the size of the messages (if we're in control of the producer) can help reducing the memory footprint, and experimenting with better serializers can help performance (see bonus below).


# 10. Edge cases, options, and misc

In all of the above, I never checked for the structure of the incoming messages. For example, my producer outputs every kind of message to the same partition, while I blindly gave for granted that I would find a `ts` field. Bad timestamps are definitely possible (as mentioned in bitflips above).

Also, I decided to auto close the consumer after 1s of inactivity, but this would depend on the source we're dealing with.

# Bonus

> How can you scale it to improve throughput?

Answered above.

> You may want count things for different time frames but only do json parsing once.

Not sure I understand this: can't I just set up counting for multiple timeframes already in the suggested implementation? I can set up different data structures (in this case a dict, but some type of Class would simplify this) where I aggregate by minute, day, year, etc and do it all in the same pass, so parsing would occur only once per message.

> Explain how you would cope with failure if the app crashes mid day / mid year.

If I understand correctly, offsets are stored on the consumer side, so if something crashed I would just resume from where I left off? Also, having sliding overlapping windows can help.

- When creating e.g. per minute statistics, how do you handle frames that arrive late or frames with a random timestamp (e.g. hit by a bitflip), describe a strategy?

Answered above.

> Make it accept data also from std-in (instead of Kafka) for rapid prototyping. (this might be helpful to have for testing anyways)

Done.

> Measure also the performance impact / overhead of the json parser

Done. Apprently, on my machine, time for just scanning through the whole colelction reduces by 67%.



# What would I have done with more time

- Try to use Docker/Kubernetes instead of running evering with command line commands directly: in my current work, these technologies are not used: there are internal equivalents in my current company, so I'm not experienced and it would have costed me a bit to set this up
- Implement the late timestamp strategy: it wouldn't have costed me much, but setting up a test dataset would have, so I didn't do it
- Experiment parallelization strategies, both on the producer side (partitions seems to be a good place to start) and on the consumer (apart from native multi-threading/processing, I would have liked to check performances for frameworks like Beam/Spark)
- Learn about Avro/Parquet and try them for serialization instead of JSON
- Explore the Consumer/Producer's `metric_reporters` option and `metrics()` methods
- More in depth benchmarking, I'm sure there are  a variety of packages out there to optimize the code
- Unit tests!

And of course, knowing the whole (Kafka) ecosystem better would have helped me derive more insights. As mentioned, I've never used it so this is based on a quick read of the documentation.
