//
// STOP!!! Make sure you're editing the TEMPLATE version of the README, in /src/docs/README_TEMPLATE.adoc
//
// Do NOT edit /README_TEMPLATE.adoc as your changes will be overwritten when the template is rendered again during
// `process-sources`.
//
// Changes made to this template, must then be rendered to the base readme, by running `mvn process-sources`
//
// To render the README directly, run `mvn asciidoc-template::build`
//


// dynamic include base for editing in IDEA
:project_root: ./
// for editing the template to see the includes, this will correctly render includes
ifeval::["{docname}" == "README_TEMPLATE"]

TIP:: Editing template file

:project_root: ../../

endif::[]


= Confluent Parallel Consumer
:icons:
:toc: macro
:toclevels: 3
:numbered: 1
:sectlinks: true
:sectanchors: true

:github_name: parallel-consumer
:base_url: https://github.com/confluentinc/{github_name}
:issues_link: {base_url}/issues


ifdef::env-github[]
:tip-caption: :bulb:
:note-caption: :information_source:
:important-caption: :heavy_exclamation_mark:
:caution-caption: :fire:
:warning-caption: :warning:
endif::[]

image:https://maven-badges.herokuapp.com/maven-central/io.confluent.parallelconsumer/parallel-consumer-parent/badge.svg?style=flat[link=https://mvnrepository.com/artifact/io.confluent.parallelconsumer/parallel-consumer-parent,Latest Parallel Consumer on Maven Central]

// Github actions disabled since codecov
//image:https://github.com/confluentinc/parallel-consumer/actions/workflows/maven.yml/badge.svg[Java 8 Unit Test GitHub] +
//^(^^full^ ^test^ ^suite^ ^currently^ ^running^ ^only^ ^on^ ^Confluent^ ^internal^ ^CI^ ^server^^)^

// travis badges temporarily disabled as travis isn't running CI currently
//image:https://travis-ci.com/astubbs/parallel-consumer.svg?branch=master["Build Status", link="https://travis-ci.com/astubbs/parallel-consumer"] image:https://codecov.io/gh/astubbs/parallel-consumer/branch/master/graph/badge.svg["Coverage",https://codecov.io/gh/astubbs/parallel-consumer]

Parallel Apache Kafka client wrapper with client side queueing, a simpler consumer/producer API with *key concurrency* and *extendable non-blocking IO* processing.

Confluent's https://www.confluent.io/confluent-accelerators/#parallel-consumer[product page for the project is here].

TIP: If you like this project, please ⭐ Star it in GitHub to show your appreciation, help us gauge popularity of the project and allocate resources.

NOTE: This is not a part of the Confluent commercial support offering, except through consulting engagements.
See the <<Support and Issues>> section for more information.

IMPORTANT: This project has been stable and reached its initial target feature set in Q1 2021.
It is actively maintained by the CSID team at Confluent.

[[intro]]
This library lets you process messages in parallel via a single Kafka Consumer meaning you can increase consumer parallelism without increasing the number of partitions in the topic you intend to process.
For many use cases this improves both throughput and latency by reducing load on your brokers.
It also opens up new use cases like extreme parallelism, external data enrichment, and queuing.

.Consume many messages _concurrently_ with a *single* consumer instance:
[source,java,indent=0]
----
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record)
        );
----

An overview article to the library can also be found on Confluent's https://www.confluent.io/blog/[blog]: https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/[Introducing the Confluent Parallel Consumer].

[#demo]
== Demo

.Relative speed demonstration
--
.Click on the animated SVG image to open the https://asciinema.org/a/404299[Asciinema.org player].
image::https://gist.githubusercontent.com/astubbs/26cccaf8b624a53ae26a52dbc00148b1/raw/cbf558b38b0aa624bd7637406579d2a8f00f51db/demo.svg[link="https://asciinema.org/a/404299"]
--

:talk_link: https://www.confluent.io/en-gb/events/kafka-summit-europe-2021/introducing-confluent-labs-parallel-consumer-client/
:talk_preview_image: https://play.vidyard.com/5MLb1Xh7joEQ7phxPxiyPK.jpg

[#talk]
== Video Overview

.Kafka Summit Europe 2021 Presentation
--
.A video presentation overview can be found {talk_link}[from the Kafka Summit Europe 2021] page for the presentatoin, along with slides.
[link = {talk_link}]
image::{talk_preview_image}[Talk]
--

'''

toc::[]

== Motivation

=== Why would I need this?

The unit of parallelism in Kafka’s consumers is the partition but sometimes you want to break away from this approach and manage parallelism yourself using threads rather than new instances of a Consumer.
Notable use cases include:

* Where partition counts are difficult to change and you need more parallelism than the current configuration allows.

* You wish to avoid over provisioning partitions in topics due to unknown future requirements.

* You wish to reduce the broker-side resource utilization associated with highly-parallel consumer groups.

* You need queue-like semantics that use message level acknowledgment, for example to process a work queue with short- and long-running tasks.

When reading the below, keep in mind that the unit of concurrency and thus performance, is restricted by the number of partitions (degree of sharding / concurrency).
Currently, you can't adjust the number of partitions in your Kafka topics without jumping through a lot of hoops, or breaking your key ordering.

==== Before

.The slow consumer situation with the raw Apache Kafka Consumer client
image::https://lucid.app/publicSegments/view/98ad200f-97b2-479b-930c-2805491b2ce7/image.png[align="center"]

==== After

.Example usage of the Parallel Consumer
image::https://lucid.app/publicSegments/view/2cb3b7e2-bfdf-4e78-8247-22ec394de965/image.png[align="center"]

=== Background

The core Kafka consumer client gives you a batch of messages to process one at a time.
Processing these in parallel on thread pools is difficult, particularly when considering offset management and strong ordering guarantees.
You also need to manage your consume loop, and commit transactions properly if using Exactly Once semantics.

This wrapper library for the Apache Kafka Java client handles all this for you, you just supply your processing function.

Another common situation where concurrent processing of messages is advantageous, is what is referred to as "competing consumers".
A pattern that is often addressed in traditional messaging systems using a shared queue.
Kafka doesn't provide native queue support and this can result in a slow processing message blocking the messages behind it in the same partition.
If <<ordering-guarantees,log ordering>> isn't a concern this can be an unwelcome bottleneck for users.
The Parallel Consumer provides a solution to this problem.

In addition, the <<http-with-vertx,Vert.x extension>> to this library supplies non-blocking interfaces, allowing higher still levels of concurrency with a further simplified interface.
Also included now is a <<project-reactor,module for>> https://projectreactor.io[Project Reactor.io].

=== FAQ

[qanda]
Why not just run more consumers?::
The typical way to address performance issues in a Kafka system, is to increase the number of consumers reading from a topic.
This is effective in many situations, but falls short in a lot too.

* Primarily: You cannot use more consumers than you have partitions available to read from.
For example, if you have a topic with five partitions, you cannot use a group with more than five consumers to read from it.
* Running more extra consumers has resource implications - each consumer takes up resources on both the client and broker side.
Each consumer adds a lot of overhead in terms of memory, CPU, and network bandwidth.
* Large consumer groups (especially many large groups) can cause a lot of strain on the consumer group coordination system, such as rebalance storms.
* Even with several partitions, you cannot achieve the performance levels obtainable by *per-key* ordered or unordered concurrent processing.
* A single slow or failing message will also still block all messages behind the problematic message, ie. the entire partition.
The process may recover, but the latency of all the messages behind the problematic one will be negatively impacted severely.

Why not run more consumers __within__ your application instance?::
* This is in some respects a slightly easier way of running more consumer instances, and in others a more complicated way.
However, you are still restricted by all the per consumer restrictions as described above.

Why not use the Vert.x library yourself in your processing loop?::
* Vert.x us used in this library to provide a non-blocking IO system in the message processing step.
Using Vert.x without using this library with *ordered* processing requires dealing with the quite complicated, and not straight forward, aspect of handling offset commits with Vert.x asynchronous processing system.
+
*Unordered* processing with Vert.x is somewhat easier, however offset management is still quite complicated, and the Parallel Consumer also provides optimizations for message-level acknowledgment in this case.
This library handles offset commits for both ordered and unordered processing cases.

=== Scenarios

Below are some real world use cases which illustrate concrete situations where the described advantages massively improve performance.

* Slow consumer systems in transactional systems (online vs offline or reporting systems)
** Notification system:
+
*** Notification processing system which sends push notifications to a user to acknowledge a two-factor authentication request on their mobile and authorising a login to a website, requires optimal end-to-end latency for a good user experience.
*** A specific message in this queue uncharacteristically takes a long time to process because the third party system is sometimes unpredictably slow to respond and so holds up the processing for *ALL* other notifications for other users that are in the same partition behind this message.
*** Using key order concurrent processing will allow notifications to proceed while this message either slowly succeeds or times out and retires.
** Slow GPS tracking system (slow HTTP service interfaces that can scale horizontally)
*** GPS tracking messages from 100,000 different field devices pour through at a high rate into an input topic.
*** For each message, the GPS location coordinates is checked to be within allowed ranges using a legacy HTTP services, dictated by business rules behind the service.
*** The service takes 50ms to process each message, however can be scaled out horizontally without restriction.
*** The input topic only has 10 partitions and for various reasons (see above) cannot be changed.
*** With the vanilla consumer, messages on each partition must be consumed one after the other in serial order.
*** The maximum rate of message processing is then:
+
`1 second / 50 ms * 10 partitions = 200 messages per second.`
*** By using this library, the 10 partitions can all be processed in key order.
+
`1 second / 50ms × 100,000 keys = 2,000,000 messages per second`
+
While the HTTP system probably cannot handle 2,000,000 messages per second, more importantly, your system is no longer the bottleneck.

** Slow CPU bound model processing for fraud prediction
*** Consider a system where message data is passed through a fraud prediction model which takes CPU cycles, instead of an external system being slow.
*** We can scale easily the number of CPUs on our virtual machine where the processing is being run, but we choose not to scale the partitions or consumers (see above).
*** By deploying onto machines with far more CPUs available, we can run our prediction model massively parallel, increasing our throughput and reducing our end-to-end response times.
* Spikey load with latency sensitive non-functional requirements
** An upstream system regularly floods our input topic daily at close of business with settlement totals data from retail outlets.
*** Situations like this are common where systems are designed to comfortably handle average day time load, but are not provisioned to handle sudden increases in traffic as they don't happen often enough to justify the increased spending on processing capacity that would otherwise remain idle.
*** Without adjusting the available partitions or running consumers, we can reduce our maximum end-to-end latency and increase throughout to get our global days outlet reports to division managers so action can be taken, before close of business.
** Natural consumer behaviour
*** Consider scenarios where bursts of data flooding input topics are generated by sudden user behaviour such as sales or television events ("Oprah" moments).
*** For example, an evening, prime-time game show on TV where users send in quiz answers on their devices.
The end-to-end latency of the responses to these answers needs to be as low as technically possible, even if the processing step is quick.
*** Instead of a vanilla client where each user response waits in a virtual queue with others to be processed, this library allows every single response to be processed in parallel.
* Legacy partition structure
** Any existing setups where we need higher performance either in throughput or latency where there are not enough partitions for needed concurrency level, the tool can be applied.
* Partition overloaded brokers
** Clusters with under-provisioned hardware and with too many partitions already - where we cannot expand partitions even if we were able to.
** Similar to the above, but from the operations perspective, our system is already over partitioned, perhaps in order to support existing parallel workloads which aren't using the tool (and so need large numbers of partitions).
** We encourage our development teams to migrate to the tool, and then being a process of actually __lowering__ the number of partitions in our partitions in order to reduce operational complexity, improve reliability and perhaps save on infrastructure costs.
* Server side resources are controlled by a different team we can't influence
** The cluster our team is working with is not in our control, we cannot change the partition setup, or perhaps even the consumer layout.
** We can use the tool ourselves to improve our system performance without touching the cluster / topic setup.
* Kafka Streams app that had a slow stage
** We use Kafka Streams for our message processing, but one of it's steps have characteristics of the above and we need better performance.
We can break out as described below into the tool for processing that step, then return to the Kafka Streams context.
* Provisioning extra machines (either virtual machines or real machines) to run multiple clients has a cost, using this library instead avoids the need for extra instances to be deployed in any respect.

== Features List

* Have massively parallel consumption processing without running hundreds or thousands of:
** Kafka consumer clients,
** topic partitions,
+
without operational burden or harming the cluster's performance
* Client side queueing system on top of Apache Kafka consumer
** Efficient individual message acknowledgement system (without local or third party external system state storage) to massively reduce (and usually completely eliminate) message replay upon failure - see <<offset_map>> section for more details
* Solution for the https://en.wikipedia.org/wiki/Head-of-line_blocking["head of line"] blocking problem where continued failure of a single message, prevents progress for messages behind it in the queue
* Per `key` concurrent processing, per partition and unordered message processing
* Offsets committed correctly, in order, of only processed messages, regardless of concurrency level or retries
* Batch support in all versions of the API to process batches of messages in parallel instead of single messages.
** Particularly useful for when your processing function can work with more than a single record at a time - e.g. sending records to an API which has a batch version like Elasticsearch
* Vert.x and Reactor.io non-blocking library integration
** Non-blocking I/O work management
** Vert.x's WebClient and general Vert.x Future support
** Reactor.io Publisher (Mono/Flux) and Java's CompletableFuture (through `Mono#fromFuture`)
* Exactly Once bulk transaction system
** When using the transactional mode, record processing that happens in parallel and produce records back to kafka get all grouped into a large batch transaction, and the offsets and records are submitted through the transactional producer, giving you Exactly once Semantics for parallel processing.
** For further information, see the <<transaction-system>> section.
* Fair partition traversal
* Zero~ dependencies (`Slf4j` and `Lombok`) for the core module
* Java 8 compatibility
* Throttle control and broker liveliness management
* Clean draining shutdown cycle
* Manual global pause / resume of all partitions, without unsubscribing from topics (useful for implementing a simplistic https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern[circuit breaker])
** Circuit breaker patterns for individual paritions or keys can be done through throwing failure exceptions in the processing function (see https://github.com/confluentinc/parallel-consumer/pull/291[PR #291 Explicit terminal and retriable exceptions] for further refinement)
** Note: Pausing of a partition is also automatic, whenever back pressure has built up on a given partition

//image:https://codecov.io/gh/astubbs/parallel-consumer/branch/master/graph/badge.svg["Coverage",https://codecov.io/gh/astubbs/parallel-consumer]
//image:https://travis-ci.com/astubbs/parallel-consumer.svg?branch=master["Build Status", link="https://travis-ci.com/astubbs/parallel-consumer"]

And more <<roadmap,to come>>!

== Performance

In the best case, you don't care about ordering at all.In which case, the degree of concurrency achievable is simply set by max thread and concurrency settings, or with the Vert.x extension, the Vert.x Vertical being used - e.g. non-blocking HTTP calls.

For example, instead of having to run 1,000 consumers to process 1,000 messages at the same time, we can process all 1,000 concurrently on a single consumer instance.

More typically though you probably still want the per key ordering grantees that Kafka provides.
For this there is the per key ordering setting.
This will limit the library from processing any message at the same time or out of order, if they have the same key.

Massively reduce message processing latency regardless of partition count for spikey workloads where there is good key distribution.
Eg 100,000 “users” all trigger an action at once.
As long as the processing layer can handle the load horizontally (e.g auto scaling web service), per message latency will be massively decreased, potentially down to the time for processing a single message, if the integration point can handle the concurrency.

For example, if you have a key set of 10,000 unique keys, and you need to call an http endpoint to process each one, you can use the per key order setting, and in the best case the system will process 10,000 at the same time using the non-blocking Vert.x HTTP client library.
The user just has to provide a function to extract from the message the HTTP call parameters and construct the HTTP request object.

=== Illustrative Performance Example

.(see link:./parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/VolumeTests.java[VolumeTests.java])
These performance comparison results below, even though are based on real performance measurement results, are for illustrative purposes.
To see how the performance of the tool is related to instance counts, partition counts, key distribution and how it would relate to the vanilla client.
Actual results will vary wildly depending upon the setup being deployed into.

For example, if you have hundreds of thousands of keys in your topic, randomly distributed, even with hundreds of partitions, with only a handful of this wrapper deployed, you will probably see many orders of magnitude performance improvements - massively out performing dozens of vanilla Kafka consumer clients.

.Time taken to process a large number of messages with a Single Parallel Consumer vs a single Kafka Consumer, for different key space sizes. As the number of unique keys in the data set increases, the key ordered Parallel Consumer performance starts to approach that of the unordered Parallel Consumer. The raw Kafka consumer performance remains unaffected by the key distribution.
image::https://docs.google.com/spreadsheets/d/e/2PACX-1vQffkAFG-_BzH-LKfGCVnytdzAHiCNIrixM6X2vF8cqw2YVz6KyW3LBXTB-lVazMAJxW0UDuFILKvtK/pubchart?oid=1691474082&amp;format=image[align="center"]

.Consumer group size effect on total processing time vs a single Parallel Consumer. As instances are added to the consumer group, it's performance starts to approach that of the single instance Parallel Consumer. Key ordering is faster than partition ordering, with unordered being the fastest.
image::https://docs.google.com/spreadsheets/d/e/2PACX-1vQffkAFG-_BzH-LKfGCVnytdzAHiCNIrixM6X2vF8cqw2YVz6KyW3LBXTB-lVazMAJxW0UDuFILKvtK/pubchart?oid=938493158&format=image[align="center"]

.Consumer group size effect on message latency vs a single Parallel Consumer. As instances are added to the consumer group, it's performance starts to approach that of the single instance Parallel Consumer.
image::https://docs.google.com/spreadsheets/d/e/2PACX-1vQffkAFG-_BzH-LKfGCVnytdzAHiCNIrixM6X2vF8cqw2YVz6KyW3LBXTB-lVazMAJxW0UDuFILKvtK/pubchart?oid=1161363385&format=image[align="center"]

As an illustrative example of relative performance, given:

* A random processing time between 0 and 5ms
* 10,000 messages to process
* A single partition (simplifies comparison - a topic with 5 partitions is the same as 1 partition with a keyspace of 5)
* Default `ParallelConsumerOptions`
** maxUncommittedMessagesToHandle = 1000
** maxConcurrency = 100
** numberOfThreads = 16

.Comparative performance of order modes and key spaces
[cols="1,1,1,3",options="header"]
|===
|Ordering
|Number of keys
|Duration
|Note

|Partition
|20 (not relevant)
|22.221s
|This is the same as a single partition with a single normal serial consumer, as we can see: 2.5ms avg processing time * 10,000 msg / 1000ms = ~25s.

|Key
|1
|26.743s
|Same as above

|Key
|2
|13.576s
|

|Key
|5
|5.916s
|

|Key
|10
|3.310s
|

|Key
|20
|2.242s
|

|Key
|50
|2.204s
|

|Key
|100
|2.178s
|

|Key
|1,000
|2.056s
|

|Key
|10,000
|2.128s
|As key space is t he same as the number of messages, this is similar (but restricted by max concurrency settings) as having a *single consumer* instance and *partition* _per key_. 10,000 msgs * avg processing time 2.5ms = ~2.5s.

|Unordered
|20 (not relevant)
|2.829s
|As there is no order restriction, this is similar (but restricted by max concurrency settings) as having a *single consumer* instance and *partition* _per key_. 10,000 msgs * avg processing time 2.5ms = ~2.5s.
|===

== Support and Issues

If you encounter any issues, or have any suggestions or future requests, please create issues in the {issues_link}[github issue tracker].
Issues will be dealt with on a good faith, best efforts basis, by the small team maintaining this library.

We also encourage participation, so if you have any feature ideas etc, please get in touch, and we will help you work on submitting a PR!

NOTE: We are very interested to hear about your experiences!
And please vote on your favourite issues!

If you have questions, head over to the https://launchpass.com/confluentcommunity[Confluent Slack community], or raise an https://github.com/confluentinc/parallel-consumer/issues[issue] on GitHub.

== License

This library is copyright Confluent Inc, and licensed under the Apache License Version 2.0.

== Usage

=== Maven

This project is available in maven central, https://repo1.maven.org/maven2/io/confluent/parallelconsumer/[repo1], along with SNAPSHOT builds (starting with 0.5-SNAPSHOT) in https://oss.sonatype.org/content/repositories/snapshots/io/confluent/parallelconsumer/[repo1's SNAPSHOTS repo].

Latest version can be seen https://search.maven.org/artifact/io.confluent.parallelconsumer/parallel-consumer-core[here].

Where `${project.version}` is the version to be used:

* group ID: `io.confluent.parallelconsumer`
* artifact ID: `parallel-consumer-core`
* version: image:https://maven-badges.herokuapp.com/maven-central/io.confluent.parallelconsumer/parallel-consumer-parent/badge.svg?style=flat[link=https://mvnrepository.com/artifact/io.confluent.parallelconsumer/parallel-consumer-parent,Latest Parallel Consumer on Maven Central]

.Core Module Dependency
[source,xml,indent=0]
        <dependency>
            <groupId>io.confluent.parallelconsumer</groupId>
            <artifactId>parallel-consumer-core</artifactId>
            <version>${project.version}</version>
        </dependency>

.Reactor Module Dependency
[source,xml,indent=0]
        <dependency>
            <groupId>io.confluent.parallelconsumer</groupId>
            <artifactId>parallel-consumer-reactor</artifactId>
            <version>${project.version}</version>
        </dependency>

.Vert.x Module Dependency
[source,xml,indent=0]
        <dependency>
            <groupId>io.confluent.parallelconsumer</groupId>
            <artifactId>parallel-consumer-vertx</artifactId>
            <version>${project.version}</version>
        </dependency>

[[common_preparation]]
=== Common Preparation

.Setup the client
[source,java,indent=0]
----
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <1>
        Producer<String, String> kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY) // <2>
                .maxConcurrency(1000) // <3>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        return eosStreamProcessor;
----

<1> Setup your clients as per normal.
A Producer is only required if using the `produce` flows.
<2> Choose your ordering type, `KEY` in this case.
This ensures maximum concurrency, while ensuring messages are processed and committed in `KEY` order, making sure no offset is committed unless all offsets before it in it's partition, are completed also.
<3> The maximum number of concurrent processing operations to be performing at any given time.
Also, because the library coordinates offsets, `enable.auto.commit` must be disabled in your consumer.
<5> Subscribe to your topics

NOTE: Because the library coordinates offsets, `enable.auto.commit` must be disabled.

After this setup, one then has the choice of interfaces:

* `ParallelStreamProcessor`
* `VertxParallelStreamProcessor`
* `JStreamParallelStreamProcessor`
* `JStreamVertxParallelStreamProcessor`

There is another interface: `ParallelConsumer` which is integrated, however there is currently no immediate implementation.
See {issues_link}/12[issue #12], and the `ParallelConsumer` JavaDoc:

[source,java]
----
/**
 * Asynchronous / concurrent message consumer for Kafka.
 * <p>
 * Currently, there is no direct implementation, only the {@link ParallelStreamProcessor} version (see
 * {@link AbstractParallelEoSStreamProcessor}), but there may be in the future.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see AbstractParallelEoSStreamProcessor
 */
----

=== Core

==== Simple Message Process

This is the only thing you need to do, in order to get massively concurrent processing in your code.

.Usage - print message content out to the console in parallel
[source,java,indent=0]
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record)
        );

See the link:./parallel-consumer-examples/parallel-consumer-example-core/src/main/java/io/confluent/parallelconsumer/examples/core/CoreApp.java[core example] project, and it's test.

==== Process and Produce a Response Message

This interface allows you to process your message, then publish back to the broker zero, one or more result messages.
You can also optionally provide a callback function to be run after the message(s) is(are) successfully published to the broker.

.Usage - print message content out to the console in parallel
[source,java,indent=0]
        parallelConsumer.pollAndProduce(context -> {
                    var consumerRecord = context.getSingleRecord().getConsumerRecord();
                    var result = processBrokerRecord(consumerRecord);
                    return new ProducerRecord<>(outputTopic, consumerRecord.key(), result.payload);
                }, consumeProduceResult -> {
                    log.debug("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                }
        );

==== Callbacks vs Streams

You have the option to either use callbacks to be notified of events, or use the `Streaming` versions of the API, which use the `java.util.stream.Stream` system:

* `JStreamParallelStreamProcessor`
* `JStreamVertxParallelStreamProcessor`

In future versions, we plan to look at supporting other streaming systems like https://github.com/ReactiveX/RxJava[RxJava] via modules.

[[batching]]
=== Batching

The library also supports sending a batch or records as input to the users processing function in parallel.
Using this, you can process several records in your function at once.

To use it, set a `batch size` in the options class.

There are then various access methods for the batch of records - see the `PollContext` object for more information.

IMPORTANT: If an exception is thrown while processing the batch, all messages in the batch will be returned to the queue, to be retried with the standard retry system.
There is no guarantee that the messages will be retried again in the same batch.

==== Usage

[source,java,indent=0]
----
        ParallelStreamProcessor.createEosStreamProcessor(ParallelConsumerOptions.<String, String>builder()
                .consumer(getKafkaConsumer())
                .producer(getKafkaProducer())
                .maxConcurrency(100)
                .batchSize(5) // <1>
                .build());
        parallelConsumer.poll(context -> {
            // convert the batch into the payload for our processing
            List<String> payload = context.stream()
                    .map(this::preparePayload)
                    .collect(Collectors.toList());
            // process the entire batch payload at once
            processBatchPayload(payload);
        });
----

<1> Choose your batch size.

==== Restrictions

- If using a batch version of the API, you must choose a batch size in the options class.
- If a batch size is chosen, the "normal" APIs cannot be used, and an error will be thrown.

[[http-with-vertx]]
=== HTTP with the Vert.x Module

.Call an HTTP endpoint for each message usage
[source,java,indent=0]
----
        var resultStream = parallelConsumer.vertxHttpReqInfoStream(context -> {
            var consumerRecord = context.getSingleConsumerRecord();
            log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return new RequestInfo("localhost", port, "/api", params); // <1>
        });
----

<1> Simply return an object representing the request, the Vert.x HTTP engine will handle the rest, using it's non-blocking engine

See the link:{project_root}/parallel-consumer-examples/parallel-consumer-example-vertx/src/main/java/io/confluent/parallelconsumer/examples/vertx/VertxApp.java[Vert.x example] project, and it's test.

[[project-reactor]]
=== Project Reactor

As per the Vert.x support, there is also a Reactor module.
This means you can use Reactor's non-blocking threading model to process your messages, allowing for orders of magnitudes higher concurrent processing than the core module's thread per worker module.

See the link:{project_root}/parallel-consumer-examples/parallel-consumer-example-reactor/src/main/java/io/confluent/parallelconsumer/examples/reactor/ReactorApp.java[Reactor example] project, and it's test.

.Call any Reactor API for each message usage. This example uses a simple `Mono.just` to return a value, but you can use any Reactor API here.
[source,java,indent=0]
----
        parallelConsumer.react(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            log.info("Concurrently constructing and returning RequestInfo from record: {}", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return Mono.just("something todo"); // <1>
        });
----

[[spring]]
[[streams-usage-code]]
=== Kafka Streams Concurrent Processing

Use your Streams app to process your data first, then send anything needed to be processed concurrently to an output topic, to be consumed by the parallel consumer.

.Example usage with Kafka Streams
image::https://lucid.app/publicSegments/view/43f2740c-2a7f-4b7f-909e-434a5bbe3fbf/image.png[Kafka Streams Usage,align="center"]

.Preprocess in Kafka Streams, then process concurrently
[source,java,indent=0]
----
    void run() {
        preprocess(); // <1>
        concurrentProcess(); // <2>
    }

    void preprocess() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .mapValues((key, value) -> {
                    log.info("Streams preprocessing key: {} value: {}", key, value);
                    return String.valueOf(value.length());
                })
                .to(outputTopicName);

        startStreams(builder.build());
    }

    void startStreams(Topology topology) {
        streams = new KafkaStreams(topology, getStreamsProperties());
        streams.start();
    }

    void concurrentProcess() {
        setupParallelConsumer();

        parallelConsumer.poll(record -> {
            log.info("Concurrently processing a record: {}", record);
            messageCount.getAndIncrement();
        });
    }
----

<1> Setup your Kafka Streams stage as per normal, performing any type of preprocessing in Kafka Streams
<2> For the slow consumer part of your Topology, drop down into the parallel consumer, and use massive concurrency

See the link:{project_root}/parallel-consumer-examples/parallel-consumer-example-streams/src/main/java/io/confluent/parallelconsumer/examples/streams/StreamsApp.java[Kafka Streams example] project, and it's test.

[[confluent-cloud]]
=== Confluent Cloud

. Provision your fully managed Kafka cluster in Confluent Cloud
.. Sign up for https://www.confluent.io/confluent-cloud/tryfree/[Confluent Cloud], a fully-managed Apache Kafka service.
.. After you log in to Confluent Cloud, click on `Add cloud environment` and name the environment `learn-kafka`.
Using a new environment keeps your learning resources separate from your other Confluent Cloud resources.
.. Click on https://confluent.cloud/learn[LEARN] and follow the instructions to launch a Kafka cluster and to enable Schema Registry.
. Access the client configuration settings
.. From the Confluent Cloud Console, navigate to your Kafka cluster.
From the `Clients` view, get the connection information customized to your cluster (select `Java`).
.. Create new credentials for your Kafka cluster, and then Confluent Cloud will show a configuration block with your new credentials automatically populated (make sure `show API keys` is checked).
.. Use these settings presented to https://docs.confluent.io/clients-kafka-java/current/overview.html[configure your clients].
. Use these clients for steps outlined in the <<common_preparation>> section.

[[upgrading]]
== Upgrading

=== From 0.4 to 0.5

This version has a breaking change in the API - instead of passing in `ConsumerRecord` instances, it passes in a `PollContext` object which has extra information and utility methods.
See the `PollContext` class for more information.

[[ordering-guarantees]]
== Ordering Guarantees

The user has the option to either choose ordered, or unordered message processing.

Either in `ordered` or `unordered` processing, the system will only commit offsets for messages which have been successfully processed.

CAUTION: `Unordered` processing could cause problems for third party integration where ordering by key is required.

CAUTION: Beware of third party systems which are not idempotent, or are key order sensitive.

IMPORTANT: The below diagrams represent a single iteration of the system and a very small number of input partitions and messages.

=== Vanilla Kafka Consumer Operation

Given this input topic with three partitions and a series of messages:

.Input topic
image::https://lucid.app/publicSegments/view/37d13382-3067-4c93-b521-7e43f2295fff/image.png[align="center"]

The normal Kafka client operations in the following manner.
Note that typically offset commits are not performed after processing a single message, but is illustrated in this manner for comparison to the single pass concurrent methods below.
Usually many messages are committed in a single go, which is much more efficient, but for our illustrative purposes is not really relevant, as we are demonstration sequential vs concurrent _processing_ messages.

.Normal execution of the raw Kafka client
image::https://lucid.app/publicSegments/view/0365890d-e8ff-4a06-b24a-8741175dacc3/image.png[align="center"]

=== Unordered

Unordered processing is where there is no restriction on the order of multiple messages processed per partition, allowing for highest level of concurrency.

This is the fastest option.

.Unordered concurrent processing of message
image::https://lucid.app/publicSegments/view/aab5d743-de05-46d0-8c1e-0646d7d2946f/image.png[align="center"]

=== Ordered by Partition

At most only one message from any given input partition will be in flight at any given time.
This means that concurrent processing is restricted to the number of input partitions.

The advantage of ordered processing mode, is that for an assignment of 1000 partitions to a single consumer, you do not need to run 1000 consumer instances or threads, to process the partitions in parallel.

Note that for a given partition, a slow processing message _will_ prevent messages behind it from being processed.
However, messages in other partitions assigned to the consumer _will_ continue processing.

This option is most like normal operation, except if the consumer is assigned more than one partition, it is free to process all partitions in parallel.

.Partition ordered concurrent processing of messages
image::https://lucid.app/publicSegments/view/30ad8632-e8fe-4e05-8afd-a2b6b3bab309/image.png[align="center"]

=== Ordered by Key

Most similar to ordered by partition, this mode ensures process ordering by *key* (per partition).

The advantage of this mode, is that a given input topic may not have many partitions, it may have a ~large number of unique keys.
Each of these key -> message sets can actually be processed concurrently, bringing concurrent processing to a per key level, without having to increase the number of input partitions, whilst keeping strong ordering by key.

As usual, the offset tracking will be correct, regardless of the ordering of unique keys on the partition or adjacency to the committed offset, such that after failure or rebalance, the system will not replay messages already marked as successful.

This option provides the performance of maximum concurrency, while maintaining message processing order per key, which is sufficient for many applications.

.Key ordering concurrent processing of messages
image::https://lucid.app/publicSegments/view/f7a05e99-24e6-4ea3-b3d0-978e306aa568/image.png[align="center"]

=== Retries and Ordering

Even during retries, offsets will always be committed only after successful processing, and in order.

== Retries

If processing of a record fails, the record will be placed back into it's queue and retried with a configurable delay (see the `ParallelConsumerOptions` class).
Ordering guarantees will always be adhered to, regardless of failure.

A failure is denoted by *any* exception being thrown from the user's processing function.
The system catches these exceptions, logs them and replaces the record in the queue for processing later.
All types of Exceptions thrown are considered retriable.
To not retry a record, do not throw an exception from your processing function.

TIP:: To avoid the system logging an error, throw an exception which extends PCRetriableException.

TIP:: If there was an error processing a record, and you'd like to skip it - do not throw an exception, and the system will mark the record as succeeded.

If for some reason you want to proactively fail a record, without relying on some other system throwing an exception which you don't catch - simply throw an exception of your own design, which the system will treat the same way.

To configure the retry delay, see `ParallelConsumerOptions#defaultRetryDelay`.

At the moment there is no terminal error support, so messages will continue to be retried forever as long as an exception continues to be thrown from the user function (see <<skipping-records>>).
But still this will not hold up the queues in `KEY` or `UNORDERED` modes, however in `PARTITION` mode it *will* block progress.
Offsets will also continue to be committed (see <<commit-mode>> and <<Offset Map>>).

=== Retry Delay Function

As part of the https://github.com/confluentinc/parallel-consumer/issues/65[enhanced retry epic], the ability to https://github.com/confluentinc/parallel-consumer/issues/82[dynamically determine the retry delay] was added.
This can be used to customise retry delay for a record, such as exponential back off or have different delays for different types of records, or have the delay determined by the status of a system etc.

You can access the retry count of a record through it's wrapped `WorkContainer` class, which is the input variable to the retry delay function.

.Example retry delay function implementing exponential backoff
[source,java,indent=0]
----
        final double multiplier = 0.5;
        final int baseDelaySecond = 1;

        ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(recordContext -> {
                    int numberOfFailedAttempts = recordContext.getNumberOfFailedAttempts();
                    long delayMillis = (long) (baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts) * 1000);
                    return Duration.ofMillis(delayMillis);
                });
----

[[skipping-records]]
=== Skipping Records

If for whatever reason you want to skip a record, simply do not throw an exception, or catch any exception being thrown, log and swallow it and return from the user function normally.
The system will treat this as a record processing success, mark the record as completed and move on as though it was a normal operation.

A user may choose to skip a record for example, if it has been retried too many times or if the record is invalid or doesn't need processing.

Implementing a https://github.com/confluentinc/parallel-consumer/issues/196[max retries feature] as a part of the system is planned.

.Example of skipping a record after a maximum number of retries is reached
[source,java,indent=0]
----
        final int maxRetries = 10;
        final Map<ConsumerRecord<String, String>, Long> retriesCount = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            Long retryCount = retriesCount.computeIfAbsent(consumerRecord, ignore -> 0L);
            if (retryCount < maxRetries) {
                processRecord(consumerRecord);
                // no exception, so completed - remove from map
                retriesCount.remove(consumerRecord);
            } else {
                log.warn("Retry count {} exceeded max of {} for record {}", retryCount, maxRetries, consumerRecord);
                // giving up, remove from map
                retriesCount.remove(consumerRecord);
            }
        });
----

=== Circuit Breaker Pattern

Although the system doesn't have an https://github.com/confluentinc/parallel-consumer/issues/110[explicit circuit breaker pattern feature], one can be created by combining the custom retry delay function and proactive failure.
For example, the retry delay can be calculated based upon the status of an external system - i.e. if the external system is currently out of action, use a higher retry.
Then in the processing function, again check the status of the external system first, and if it's still offline, throw an exception proactively without attempting to process the message.
This will put the message back in the queue.

.Example of circuit break implementation
[source,java,indent=0]
----
        final Map<String, Boolean> upMap = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            String serverId = extractServerId(consumerRecord);
            boolean up = upMap.computeIfAbsent(serverId, ignore -> true);

            if (!up) {
                up = updateStatusOfSever(serverId);
            }

            if (up) {
                try {
                    processRecord(consumerRecord);
                } catch (CircuitBreakingException e) {
                    log.warn("Server {} is circuitBroken, will retry message when server is up. Record: {}", serverId, consumerRecord);
                    upMap.put(serverId, false);
                }
                // no exception, so set server status UP
                upMap.put(serverId, true);
            } else {
                throw new RuntimeException(msg("Server {} currently down, will retry record latter {}", up, consumerRecord));
            }
        });
----

=== Head of Line Blocking

In order to have a failing record not block progress of a partition, one of the ordering modes other than `PARTITION` must be used, so that the system is allowed to process other messages that are perhaps in `KEY` order or in the case of `UNORDERED` processing - any message.
This is because in `PARTITION` ordering mode, records are always processed in order of partition, and so the Head of Line blocking feature is effectively disabled.

=== Future Work

Improvements to this system are planned, see the following issues:

* https://github.com/confluentinc/parallel-consumer/issues/65[Enhanced retry epic #65]
* https://github.com/confluentinc/parallel-consumer/issues/48[Support scheduled message processing (scheduled retry)]
* https://github.com/confluentinc/parallel-consumer/issues/196[Provide option for max retires, and a call back when reached (potential DLQ) #196]
* https://github.com/confluentinc/parallel-consumer/issues/34[Monitor for progress and optionally shutdown (leave consumer group), skip message or send to DLQ #34]

== Result Models

* Void

Processing is complete simply when your provided function finishes, and the offsets are committed.

* Streaming User Results

When your function is actually run, a result object will be streamed back to your client code, with information about the operation completion.

* Streaming Message Publishing Results

After your operation completes, you can also choose to publish a result message back to Kafka.
The message publishing metadata can be streamed back to your client code.

[[commit-mode]]
== Commit Mode

The system gives you three choices for how to do offset commits.
The simplest of the three are the two Consumer commits modes.
They are of course, `synchronous` and `asynchronous` mode.
The `transactional` mode is explained in the next section.

`Asynchronous` mode is faster, as it doesn't block the control loop.

`Synchronous` will block the processing loop until a successful commit response is received, however, `Asynchronous` will still be capped by the max processing settings in the `ParallelConsumerOptions` class.

If you're used to using the auto commit mode in the normal Kafka consumer, you can think of the `Asynchronous` mode being similar to this.
We suggest starting with this mode, and it is the default.

[[transaction-system]]
=== Apache Kafka EoS Transaction Model in BULK

There is also the option to use Kafka's Exactly Once Semantics (EoS) system.
This causes all messages produced, by all workers in parallel, as a result of processing their messages, to be committed within a SINGLE, BULK transaction, along with their source offset.

Note importantly - this is a BULK transaction, not a per input record transaction.

This means that even under failure, the results will exist exactly once in the Kafka output topic.
If as a part of your processing, you create side effects in other systems, this pertains to the usual idempotency requirements when breaking of EoS Kafka boundaries.

CAUTION:: This is a BULK transaction, not a per input record transaction.
There is not a single transaction per input record and per worker "thread", but one *LARGE* transaction that gets used by all parallel processing, until the commit interval.

NOTE:: As with the `synchronous` processing mode, this will also block the processing loop until a successful transaction completes

CAUTION: This cannot be true for any externally integrated third party system, unless that system is __idempotent__.

For implementations details, see the <<Transactional System Architecture>> section.

.From the Options Javadoc
[source,java,indent=0]
----
        /**
         * Periodically commits through the Producer using transactions.
         * <p>
         * Messages sent in parallel by different workers get added to the same transaction block - you end up with
         * transactions 100ms (by default) "large", containing all records sent during that time period, from the
         * offsets being committed.
         * <p>
         * Of no use, if not also producing messages (i.e. using a {@link ParallelStreamProcessor#pollAndProduce}
         * variation).
         * <p>
         * Note: Records being sent by different threads will all be in a single transaction, as PC shares a single
         * Producer instance. This could be seen as a performance overhead advantage, efficient resource use, in
         * exchange for a loss in transaction granularity.
         * <p>
         * The benefits of using this mode are:
         * <p>
         * a) All records produced from a given source offset will either all be visible, or none will be
         * ({@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}).
         * <p>
         * b) If any records making up a transaction have a terminal issue being produced, or the system crashes before
         * finishing sending all the records and committing, none will ever be visible and the system will eventually
         * retry them in new transactions - potentially with different combinations of records from the original.
         * <p>
         * c) A source offset, and it's produced records will be committed as an atomic set. Normally: either the record
         * producing could fail, or the committing of the source offset could fail, as they are separate individual
         * operations. When using Transactions, they are committed together - so if either operations fails, the
         * transaction will never get committed, and upon recovery, the system will retry the set again (and no
         * duplicates will be visible in the topic).
         * <p>
         * This {@code CommitMode} is the slowest of the options, but there will be no duplicates in Kafka caused by
         * producing a record multiple times if previous offset commits have failed or crashes have occurred (however
         * message replay may cause duplicates in external systems which is unavoidable - external systems must be
         * idempotent).
         * <p>
         * The default commit interval {@link AbstractParallelEoSStreamProcessor#KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY}
         * gets automatically reduced from the default of 5 seconds to 100ms (the same as Kafka Streams <a
         * href=https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html">commit.interval.ms</a>).
         * Reducing this configuration places higher load on the broker, but will reduce (but cannot eliminate) replay
         * upon failure. Note also that when using transactions in Kafka, consumption in {@code READ_COMMITTED} mode is
         * blocked up to the offset of the first STILL open transaction. Using a smaller commit frequency reduces this
         * minimum consumption latency - the faster transactions are closed, the faster the transaction content can be
         * read by {@code READ_COMMITTED} consumers. More information about this can be found on the Confluent blog
         * post:
         * <a href="https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/">Enabling Exactly-Once in Kafka
         * Streams</a>.
         * <p>
         * When producing multiple records (see {@link ParallelStreamProcessor#pollAndProduceMany}), all records must
         * have been produced successfully to the broker before the transaction will commit, after which all will be
         * visible together, or none.
         * <p>
         * Records produced while running in this mode, won't be seen by consumer running in
         * {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} {@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}
         * mode until the transaction is complete and all records are produced successfully. Records produced into a
         * transaction that gets aborted or timed out, will never be visible.
         * <p>
         * The system must prevent records from being produced to the brokers whose source consumer record offsets has
         * not been included in this transaction. Otherwise, the transactions would include produced records from
         * consumer offsets which would only be committed in the NEXT transaction, which would break the EoS guarantees.
         * To achieve this, first work processing and record producing is suspended (by acquiring the commit lock -
         * see{@link #commitLockAcquisitionTimeout}, as record processing requires the produce lock), then succeeded
         * consumer offsets are gathered, transaction commit is made, then when the transaction has finished, processing
         * resumes by releasing the commit lock. This periodically slows down record production during this phase, by
         * the time needed to commit the transaction.
         * <p>
         * This is all separate from using an IDEMPOTENT Producer, which can be used, along with the
         * {@link ParallelConsumerOptions#commitMode} {@link CommitMode#PERIODIC_CONSUMER_SYNC} or
         * {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}.
         * <p>
         * Failure:
         * <p>
         * Commit lock: If the system cannot acquire the commit lock in time, it will shut down for whatever reason, the
         * system will shut down (fail fast) - during the shutdown a final commit attempt will be made. The default
         * timeout for acquisition is very high though - see {@link #commitLockAcquisitionTimeout}. This can be caused
         * by the user processing function taking too long to complete.
         * <p>
         * Produce lock: If the system cannot acquire the produce lock in time, it will fail the record processing and
         * retry the record later. This can be caused by the controller taking too long to commit for some reason. See
         * {@link #produceLockAcquisitionTimeout}. If using {@link #allowEagerProcessingDuringTransactionCommit}, this
         * may cause side effect replay when the record is retried, otherwise there is no replay. See
         * {@link #allowEagerProcessingDuringTransactionCommit} for more details.
         *
         * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval
         */
----

[[streams-usage]]
== Using with Kafka Streams

Kafka Streams (KS) doesn't yet (https://cwiki.apache.org/confluence/display/KAFKA/KIP-311%3A+Async+processing+with+dynamic+scheduling+in+Kafka+Streams[KIP-311],
https://cwiki.apache.org/confluence/display/KAFKA/KIP-408%3A+Add+Asynchronous+Processing+To+Kafka+Streams[KIP-408]) have parallel processing of messages.
However, any given preprocessing can be done in KS, preparing the messages.
One can then use this library to consume from an input topic, produced by KS to process the messages in parallel.

For a code example, see the <<streams-usage-code>> section.

.Example usage with Kafka Streams
image::https://lucid.app/publicSegments/view/43f2740c-2a7f-4b7f-909e-434a5bbe3fbf/image.png[Kafka Streams Usage,align="center"]

[[roadmap]]
== Roadmap

For released changes, see the link:CHANGELOG.adoc[CHANGELOG].
For features in development, have a look at the https://github.com/confluentinc/parallel-consumer/issues[GitHub issues].

=== Medium Term - What's up next ⏲

* https://github.com/confluentinc/parallel-consumer/issues/28[Distributed tracing integration]
* https://github.com/confluentinc/parallel-consumer/issues/24[Distributed rate limiting]
* https://github.com/confluentinc/parallel-consumer/issues/27[Metrics]
* More customisable handling[https://github.com/confluentinc/parallel-consumer/issues/65] of HTTP interactions

=== Long Term - The future ☁️

* https://github.com/confluentinc/parallel-consumer/issues/21[Automatic fanout] (automatic selection of concurrency level based on downstream back pressure) (https://github.com/confluentinc/parallel-consumer/pull/22[draft PR])
* Dead Letter Queue (DLQ) handling
* Call backs only once offset has been committed

== Usage Requirements

* Client side
** JDK 8
** SLF4J
** Apache Kafka (AK) Client libraries 2.5
** Supports all features of the AK client (e.g. security setups, schema registry etc)
** For use with Streams, see <<streams-usage>> section
** For use with Connect:
*** Source: simply consume from the topic that your Connect plugin is publishing to
*** Sink: use the poll and producer style API and publish the records to the topic that the connector is sinking from
* Server side
** Should work with any cluster that the linked AK client library works with
*** If using EoS/Transactions, needs a cluster setup that supports EoS/transactions

== Development Information

=== Requirements

* Uses https://projectlombok.org/setup/intellij[Lombok], if you're using IntelliJ Idea, get the https://plugins.jetbrains.com/plugin/6317-lombok[plugin].
* Integration tests require a https://docs.docker.com/docker-for-mac/[running locally accessible Docker host].
* Has a Maven `profile` setup for IntelliJ Idea, but not Eclipse for example.

=== Notes

The unit test code is set to run at a very high frequency, which can make it difficult to read debug logs (or impossible).
If you want to debug the code or view the main logs, consider changing the below:

// replace with code inclusion from readme branch
.ParallelEoSStreamProcessorTestBase
[source]
----
ParallelEoSStreamProcessorTestBase#DEFAULT_BROKER_POLL_FREQUENCY_MS
ParallelEoSStreamProcessorTestBase#DEFAULT_COMMIT_INTERVAL_MAX_MS
----

=== Recommended IDEA Plugins

* AsciiDoc
* CheckStyle
* CodeGlance
* EditorConfig
* Rainbow Brackets
* SonarLint
* Lombok

=== Readme

The `README` uses a special https://github.com/whelk-io/asciidoc-template-maven-plugin/pull/25[custom maven processor plugin] to import live code blocks into the root readme, so that GitHub can show the real code as includes in the `README`.
This is because GitHub https://github.com/github/markup/issues/1095[doesn't properly support the _include_ directive].

The source of truth readme is in link:{project_root}/src/docs/README_TEMPLATE.adoc[].

=== Maven targets

[qanda]
Compile and run all tests::
`mvn verify`

Run tests excluding the integration tests::
`mvn test`

Run all tests::
`mvn verify`

Run any goal skipping tests (replace `<goalName>` e.g. `install`)::
`mvn <goalName> -DskipTests`

See what profiles are active::
`mvn help:active-profiles`

See what plugins or dependencies are available to be updated::
`mvn versions:display-plugin-updates versions:display-property-updates versions:display-dependency-updates`

Run a single unit test::
`mvn -Dtest=TestCircle test`

Run a specific integration test method in a submodule project, skipping unit tests::
`mvn -Dit.test=TransactionAndCommitModeTest#testLowMaxPoll -DskipUTs=true verify  -DfailIfNoTests=false --projects parallel-consumer-core`

Run `git bisect` to find a bad commit, edit the Maven command in `bisect.sh` and run::

[source=bash]
----
git bisect start good bad
git bisect run ./bisect.sh
----

Note::
`mvn compile` - Due to a bug in Maven's handling of test-jar dependencies - running `mvn compile` fails, use `mvn test-compile` instead.
See https://github.com/confluentinc/parallel-consumer/issues/162[issue #162]
and this https://stackoverflow.com/questions/4786881/why-is-test-jar-dependency-required-for-mvn-compile[Stack Overflow question].

=== Testing

The project has good automated test coverage, of all features.
Including integration tests running against real Kafka broker and database.
If you want to run the tests yourself, clone the repository and run the command: `mvn test`.
The tests require an active docker server on `localhost`.

==== Integration Testing with TestContainers
//https://github.com/confluentinc/schroedinger#integration-testing-with-testcontainers

We use the excellent https://testcontainers.org[Testcontainers] library for integration testing with JUnit.

To speed up test execution, you can enable container reuse across test runs by setting the following in your https://www.testcontainers.org/features/configuration/[`~/.testcontainers.properties` file]:

[source]
----
testcontainers.reuse.enable=true
----

This will leave the container running after the JUnit test is complete for reuse by subsequent runs.

> NOTE: The container will only be left running if it is not explicitly stopped by the JUnit rule.
> For this reason, we use a variant of the https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers[singleton container pattern]
> instead of the JUnit rule.

Testcontainers detects if a container is reusable by hashing the container creation parameters from the JUnit test.
If an existing container is _not_ reusable, a new container will be created, **but the old container will not be removed**.

Target | Description --- | ---
`testcontainers-list` | List all containers labeled as testcontainers
`testcontainers-clean` | Remove all containers labeled as testcontainers

.Stop and remove all containers labeled with `org.testcontainers=true`
[source,bash]
----
docker container ls --filter 'label=org.testcontainers=true' --format '{{.ID}}' \
| $(XARGS) docker container rm --force
----

.List all containers labeled with `org.testcontainers=true`
[source,bash]
----
docker container ls --filter 'label=org.testcontainers=true'
----

> NOTE: `testcontainers-clean` removes **all** docker containers on your system with the `io.testcontainers=true` label > (including the most recent container which may be reusable).

See https://github.com/testcontainers/testcontainers-java/pull/1781[this testcontainers PR] for details on the reusable containers feature.

== Implementation Details

=== Core Architecture

Concurrency is controlled by the size of the thread pool (`worker pool` in the diagram).
Work is performed in a blocking manner, by the users submitted lambda functions.

These are the main sub systems:

- controller thread
- broker poller thread
- work pool thread
- work management
- offset map manipulation

Each thread collaborates with the others through thread safe Java collections.

.Core Architecture. Threads are represented by letters and colours, with their steps in sequential numbers.
image::https://lucid.app/publicSegments/view/320d924a-6517-4c54-a72e-b1c4b22e59ed/image.png[Core Architecture,align="center"]

=== Vert.x Architecture

The Vert.x module is an optional extension to the core module.
As depicted in the diagram, the architecture extends the core architecture.

Instead of the work thread pool count being the degree of concurrency, it is controlled by a max parallel requests setting, and work is performed asynchronously on the Vert.x engine by a _core_ count aligned Vert.x managed thread pool using Vert.x asynchronous IO plugins (https://vertx.io/docs/vertx-core/java/#_verticles[verticles]).

.Vert.x Architecture
image::https://lucid.app/publicSegments/view/509df410-5997-46be-98e7-ac7f241780b4/image.png[Vert.x Architecture,align="center"]

=== Transactional System Architecture

image::https://lucid.app/publicSegments/view/7480d948-ed7d-4370-a308-8ec12e6b453b/image.png[]

[[offset_map]]
=== Offset Map

Unlike a traditional queue, messages are not deleted on an acknowledgement.
However, offsets *are* tracked *per message*, per consumer group - there is no message replay for successful messages, even over clean restarts.

Across a system failure, only completed messages not stored as such in the last offset payload commit will be replayed.
This is not an _exactly once guarantee_, as message replay cannot be prevented across failure.

CAUTION: Note that Kafka's Exactly Once Semantics (EoS) (transactional processing) also does not prevent _duplicate message replay_ - it *presents* an _effectively once_ result messages in Kafka topics.
Messages may _still_ be replayed when using `EoS`.
This is an important consideration when using it, especially when integrating with thrid party systems, which is a very common pattern for utilising this project.

As mentioned previously, offsets are always committed in the correct order and only once all previous messages have been successfully processed; regardless of <<ordering-guarantees,ordering mode>> selected.
We call this the "highest committable offset".

However, because messages can be processed out of order, messages beyond the highest committable offset must also be tracked for success and not replayed upon restart of failure.
To achieve this the system goes a step further than normal Kafka offset commits.

When messages beyond the highest committable offset are successfully processed;

. they are stored as such in an internal memory map.
. when the system then next commits offsets
. if there are any messages beyond the highest offset which have been marked as succeeded
.. the offset map is serialised and encoded into a base 64 string, and added to the commit message metadata.
. upon restore, if needed, the system then deserializes this offset map and loads it back into memory
. when each messages is polled into the system
.. it checks if it's already been previously completed
.. at which point it is then skipped.

This ensures that no message is reprocessed if it's been previously completed.

IMPORTANT: Successful messages beyond the _highest committable offset_ are still recorded as such in a specially constructed metadata payload stored alongside the Kafka committed offset.
These messages are not replayed upon restore/restart.

The offset map is compressed in parallel using two different compression techniques - run length encoding and bitmap encoding.
The sizes of the compressed maps are then compared, and the smallest chosen for serialization.
If both serialised formats are significantly large, they are then both compressed using `zstd` compression, and if that results in a smaller serialization then the compressed form is used instead.


==== Storage Notes

* Runtime data model creates list of incomplete offsets
* Continuously builds a full complete / not complete bit map from the base offset to be committed
* Dynamically switching storage
** encodes into a `BitSet`, and a `RunLength`, then compresses both using zstd, then uses the smallest and tags as such in the encoded String
** Which is smallest can depend on the size and information density of the offset map
*** Smaller maps fit better into uncompressed `BitSets` ~(30 entry map bitset: compressed: 13 Bytes, uncompressed: 4 Bytes)
*** Larger maps with continuous sections usually better in compressed `RunLength`
*** Completely random offset maps, compressed and uncompressed `BitSet` is roughly the same (2000 entries, uncompressed bitset: 250, compressed: 259, compressed bytes array: 477)
*** Very large maps (20,000 entries), a compressed `BitSet` seems to be significantly smaller again if random.
* Gets stored along with base offset for each partition, in the offset `commitsync` `metadata` string
* The offset commit metadata has a hardcoded limit of 4096 bytes (4 kb) per partition (@see `kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = 4096`)
** Because of this, if our map doesn't fit into this, we have to drop it and not use it, losing the shorter replay benefits.
However, with runlength encoding and typical offset patterns this should be quite rare.
*** Work is being done on continuous and predictive space requirements, which will optionally prevent the system from continuing past a point by introducing local backpressure which it can't proceed without dropping the encoded map information - see https://github.com/confluentinc/parallel-consumer/issues/53[Exact continuous offset encoding for precise offset payload size back pressure].
** Not being able to fit the map into the metadata, depends on message acknowledgement patterns in the use case and the numbers of messages involved.
Also, the information density in the map (i.e. a single not yet completed message in 4000 completed ones will be a tiny map and will fit very large amounts of messages)

===== FAQ

[qanda]
If for example, offset 5 cannot be processed for whatever reason, does it cause the committed offset to stick to 5?::
Yes - the committed offset would "stick" to 5, with the metadata payload containing all the per msg ack's beyond 5.
+
(Reference: https://github.com/confluentinc/parallel-consumer/issues/415#issuecomment-1256022394[#415])

In the above scenario, would the system eventually exceed the OffsetMap size limit?::
No, as if the payload size hits 75% or more of the limit (4kB), the back pressure system kicks in, and no more records will be taken for processing, until it drops below 75% again.
Instead, it will keep retrying existing records.
+
However, note that if the only record to continually fail is 5, and all others succeed, let's say offset 6-50,000, then the metadata payload is only ~2 shorts (1 and (50,000-6=) 49,994), as it will use run length encoding.
So it's very efficient.
+
(Reference: https://github.com/confluentinc/parallel-consumer/issues/415#issuecomment-1256022394[#415])

== Attribution

http://www.apache.org/[Apache®], http://kafka.apache.org/[Apache Kafka], and http://kafka.apache.org/[Kafka®] are either registered trademarks or trademarks of the http://www.apache.org/[Apache Software Foundation] in the United States and/or other countries.

:leveloffset: +1
:toc: macro
:toclevels: 1

= Change Log

A high level summary of noteworthy changes in each version.

NOTE:: Dependency version bumps are not listed here.

// git log --pretty="* %s" 0.3.0.2..HEAD

// only show TOC if this is the root document (not in the README)
ifndef::github_name[]
toc::[]
endif::[]

== 0.5.2.5

=== Fixes

* fixes: #195 NoSuchFieldException when using consumer inherited from KafkaConsumer (#469)

== 0.5.2.4

=== Improvements

* feature: Simple PCRetriableException to remove error spam from logs (#444)

=== Fixes

* fixes #409: Adds support for compacted topics and commit offset resetting (#425)
** Truncate the offset state when bootstrap polled offset higher or lower than committed
** Prune missing records from the tracked incomplete offset state, when they're missing from polled batches
* fix: Improvements to encoding ranges (int vs long) #439
** Replace integer offset references with long - use Long everywhere we deal with offsets, and where we truncate down, do it exactly, detect and handle truncation issues.

== 0.5.2.3

=== Improvements

* Transactional commit mode system improvements and docs (#355)
** Clarifies transaction system with much better documentation.
** Fixes a potential race condition which could cause offset leaks between transactions boundaries.
** Introduces lock acquisition timeouts.
** Fixes a potential issue with removing records from the retry queue incorrectly, by having an inconsistency between compareTo and equals in the retry TreeMap.
* Adds a very simple Dependency Injection system modeled on Dagger (#398)
* Various refactorings e.g. new ProducerWrap

* Dependencies
** build(deps): prod: zstd, reactor, dev: podam, progressbar, postgresql maven-plugins: versions, help (#420)
** build(deps-dev): bump postgresql from 42.4.1 to 42.5.0
** bump podam, progressbar, zstd, reactor
** build(deps): bump versions-maven-plugin from 2.11.0 to 2.12.0
** build(deps): bump maven-help-plugin from 3.2.0 to 3.3.0
** build(deps-dev): bump Confluent Platform Kafka Broker to 7.2.2 (#421)
** build(deps): Upgrade to AK 3.3.0 (#309)


=== Fixes

* fixes #419: NoSuchElementException during race condition in PartitionState (#422)
* Fixes #412: ClassCastException with retryDelayProvider (#417)
* fixes ShardManager retryQueue ordering and set issues due to poor Comparator implementation (#423)


== v0.5.2.2

=== Fixes

- Fixes dependency scope for Mockito from compile to test (#376)

== v0.5.2.1

=== Fixes

- Fixes regression issue with order of state truncation vs commit (#362)

== v0.5.2.0

=== Fixes and Improvements

- fixes #184: Fix multi topic subscription with KEY order by adding topic to shard key (#315)
- fixes #329: Committing around transaction markers causes encoder to crash (#328)
- build: Upgrade Truth-Generator to 0.1.1 for user Subject discovery (#332)

=== Build

- build: Allow snapshots locally, fail in CI (#331)
- build: OSS Index scan change to warn only and exclude Guava CVE-2020-8908 as it's WONT_FIX (#330)

=== Dependencies

- build(deps): bump reactor-core from 3.4.19 to 3.4.21 (#344)
- build(deps): dependabot bump Mockito, Surefire, Reactor, AssertJ, Release (#342) (#342)
- build(deps): dependabot bump TestContainers, Vert.x, Enforcer, Versions, JUnit, Postgress (#336)

=== Linked issues

- Message with null key lead to continuous failure when using KEY ordering #318
- Subscribing to two or more topics with KEY ordering, results in messages of the same Key never being processed #184
- Cannot have negative length BitSet error - committing transaction adjacent offsets #329

== v0.5.1.0

=== Features

* #193: Pause / Resume PC (circuit breaker) without unsubscribing from topics

=== Fixes and Improvements

* #225: Build and runtime support for Java 16+ (#289)
* #306: Change Truth-Generator dependency from compile to test
* #298: Improve PollAndProduce performance by first producing all records, and then waiting for the produce results.Previously, this was done for each ProduceRecord individually.

== v0.5.0.0

=== Features

* feature: Poll Context object for API (#223)
** PollContext API - provides central access to result set with various convenience methods as well as metadata about records, such as failure count
* major: Batching feature and Event system improvements
** Batching - all API methods now support batching.
See the Options class set batch size for more information.

=== Fixes and Improvements

* Event system - better CPU usage in control thread
* Concurrency stability improvements
* Update dependencies
* #247: Adopt Truth-Generator (#249)
** Adopt https://github.com/astubbs/truth-generator[Truth Generator] for automatic generation of https://truth.dev/[Google Truth] Subjects
* Large rewrite of internal architecture for improved maintence and simplicity which fixed some corner case issues
** refactor: Rename PartitionMonitor to PartitionStateManager (#269)
** refactor: Queue unification (#219)
** refactor: Partition state tracking instead of search (#218)
** refactor: Processing Shard object
* fix: Concurrency and State improvements (#190)

=== Build

* build: Lock TruthGenerator to 0.1 (#272)
* build: Deploy SNAPSHOTS to maven central snaphots repo (#265)
* build: Update Kafka to 3.1.0 (#229)
* build: Crank up Enforcer rules and turn on ossindex audit
* build: Fix logback dependency back to stable
* build: Upgrade TestContainer and CP

== v0.4.0.1

=== Improvements

- Add option to specify timeout for how long to wait offset commits in periodic-consumer-sync commit-mode
- Add option to specify timeout for how long to wait for blocking Producer#send

=== Docs

- docs: Confluent Cloud configuration links
- docs: Add Confluent's product page for PC to README
- docs: Add head of line blocking to README

== v0.4.0.0
// https://github.com/confluentinc/parallel-consumer/releases/tag/0.4.0.0

=== Features

* https://projectreactor.io/[Project Reactor] non-blocking threading adapter module
* Generic Vert.x Future support - i.e. FileSystem, db etc...

=== Fixes and Improvements

* Vert.x concurrency control via WebClient host limits fixed - see #maxCurrency
* Vert.x API cleanup of invalid usage
* Out of bounds for empty collections
* Use ConcurrentSkipListMap instead of TreeMap to prevent concurrency issues under high pressure
* log: Show record topic in slow-work warning message

== v0.3.2.0

=== Fixes and Improvements

* Major: Upgrade to Apache Kafka 2.8 (still compatible with 2.6 and 2.7 though)
* Adds support for managed executor service (Java EE Compatibility feature)
* #65 support for custom retry delay providers

== v0.3.1.0

=== Fixes and Improvements

* Major refactor to code base - primarily the two large God classes
** Partition state now tracked separately
** Code moved into packages
* Busy spin in some cases fixed (lower CPU usage)
* Reduce use of static data for test assertions - remaining identified for later removal
* Various fixes for parallel testing stability

== v0.3.0.3

=== Fixes and Improvements

==== Overview

* Tests now run in parallel
* License fixing / updating and code formatting
* License format runs properly now when local, check on CI
* Fix running on Windows and Linux
* Fix JAVA_HOME issues

==== Details:

* tests: Enable the fail fast feature now that it's merged upstream
* tests: Turn on parallel test runs
* format: Format license, fix placement
* format: Apply Idea formatting (fix license layout)
* format: Update mycila license-plugin
* test: Disable redundant vert.x test - too complicated to fix for little gain
* test: Fix thread counting test by closing PC @After
* test: Test bug due to static state overrides when run as a suite
* format: Apply license format and run every All Idea build
* format: Organise imports
* fix: Apply license format when in dev laptops - CI only checks
* fix: javadoc command for various OS and envs when JAVA_HOME missing
* fix: By default, correctly run time JVM as jvm.location

== v0.3.0.2

=== Fixes and Improvements

* ci: Add CODEOWNER
* fix: #101 Validate GroupId is configured on managed consumer
* Use 8B1DA6120C2BF624 GPG Key For Signing
* ci: Bump jdk8 version path
* fix: #97 Vert.x thread and connection pools setup incorrect
* Disable Travis and Codecov
* ci: Apache Kafka and JDK build matrix
* fix: Set Serdes for MockProducer for AK 2.7 partition fix KAFKA-10503 to fix new NPE
* Only log slow message warnings periodically, once per sweep
* Upgrade Kafka container version to 6.0.2
* Clean up stalled message warning logs
* Reduce log-level if no results are returned from user-function (warn -> debug)
* Enable java 8 Github
* Fixes #87 - Upgrade UniJ version for UnsupportedClassVersion error
* Bump TestContainers to stable release to specifically fix #3574
* Clarify offset management capabilities

== v0.3.0.1

* fixes #62: Off by one error when restoring offsets when no offsets are encoded in metadata
* fix: Actually skip work that is found as stale

== v0.3.0.0

=== Features

* Queueing and pressure system now self tuning, performance over default old tuning values (`softMaxNumberMessagesBeyondBaseCommitOffset` and `maxMessagesToQueue`) has doubled.
** These options have been removed from the system.
* Offset payload encoding back pressure system
** If the payload begins to take more than a certain threshold amount of the maximum available, no more messages will be brought in for processing, until the space need beings to reduce back below the threshold.
This is to try to prevent the situation where the payload is too large to fit at all, and must be dropped entirely.
** See Proper offset encoding back pressure system so that offset payloads can't ever be too large https://github.com/confluentinc/parallel-consumer/issues/47[#47]
** Messages that have failed to process, will always be allowed to retry, in order to reduce this pressure.

=== Improvements

* Default ordering mode is now `KEY` ordering (was `UNORDERED`).
** This is a better default as it's the safest mode yet high performing mode.
It maintains the partition ordering characteristic that all keys are processed in log order, yet for most use cases will be close to as fast as `UNORDERED` when the key space is large enough.
* https://github.com/confluentinc/parallel-consumer/issues/37[Support BitSet encoding lengths longer than Short.MAX_VALUE #37] - adds new serialisation formats that supports wider range of offsets - (32,767 vs 2,147,483,647) for both BitSet and run-length encoding.
* Commit modes have been renamed to make it clearer that they are periodic, not per message.
* Minor performance improvement, switching away from concurrent collections.

=== Fixes

* Maximum offset payload space increased to correctly not be inversely proportional to assigned partition quantity.
* Run-length encoding now supports compacted topics, plus other bug fixes as well as fixes to Bitset encoding.

== v0.2.0.3

=== Fixes

** https://github.com/confluentinc/parallel-consumer/issues/35[Bitset overflow check (#35)] - gracefully drop BitSet or Runlength encoding as an option if offset difference too large (short overflow)
*** A new serialisation format will be added in next version - see https://github.com/confluentinc/parallel-consumer/issues/37[Support BitSet encoding lengths longer than Short.MAX_VALUE #37]
** Gracefully drops encoding attempts if they can't be run
** Fixes a bug in the offset drop if it can't fit in the offset metadata payload

== v0.2.0.2

=== Fixes

** Turns back on the https://github.com/confluentinc/parallel-consumer/issues/35[Bitset overflow check (#35)]

== v0.2.0.1 DO NOT USE - has critical bug

=== Fixes

** Incorrectly turns off an over-flow check in https://github.com/confluentinc/parallel-consumer/issues/35[offset serialisation system (#35)]

== v0.2.0.0

=== Features

** Choice of commit modes: Consumer Asynchronous, Synchronous and Producer Transactions
** Producer instance is now optional
** Using a _transactional_ Producer is now optional
** Use the Kafka Consumer to commit `offsets` Synchronously or Asynchronously

=== Improvements

** Memory performance - garbage collect empty shards when in KEY ordering mode
** Select tests adapted to non transactional (multiple commit modes) as well
** Adds supervision to broker poller
** Fixes a performance issue with the async committer not being woken up
** Make committer thread revoke partitions and commit
** Have onPartitionsRevoked be responsible for committing on close, instead of an explicit call to commit by controller
** Make sure Broker Poller now drains properly, committing any waiting work

=== Fixes

** Fixes bug in commit linger, remove genesis offset (0) from testing (avoid races), add ability to request commit
** Fixes #25 https://github.com/confluentinc/parallel-consumer/issues/25:
*** Sometimes a transaction error occurs - Cannot call send in state COMMITTING_TRANSACTION #25
** ReentrantReadWrite lock protects non-thread safe transactional producer from incorrect multithreaded use
** Wider lock to prevent transaction's containing produced messages that they shouldn't
** Must start tx in MockProducer as well
** Fixes example app tests - incorrectly testing wrong thing and MockProducer not configured to auto complete
** Add missing revoke flow to MockConsumer wrapper
** Add missing latch timeout check

== v0.1

=== Features:

** Have massively parallel consumption processing without running hundreds or thousands of
*** Kafka consumer clients
*** topic partitions
+
without operational burden or harming the clusters performance
** Efficient individual message acknowledgement system (without local or third system state) to massively reduce message replay upon failure
** Per `key` concurrent processing, per `partition` and unordered message processing
** `Offsets` committed correctly, in order, of only processed messages, regardless of concurrency level or retries
** Vert.x non-blocking library integration (HTTP currently)
** Fair partition traversal
** Zero~ dependencies (`Slf4j` and `Lombok`) for the core module
** Java 8 compatibility
** Throttle control and broker liveliness management
** Clean draining shutdown cycle
//:leveloffset: -1 - Duplicate key leveloffset (attempted merging values +1 and -1): https://github.com/whelk-io/asciidoc-template-maven-plugin/issues/118
