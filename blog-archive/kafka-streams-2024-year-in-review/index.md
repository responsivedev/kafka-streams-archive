# Kafka Streams Wrapped: 2024 in review

*Sophie Blee-Goldman · December 19, 2024 · 8 min read*

Kafka Streams had some tremendous additions and improvements in 2024. Here are the highlights.

![Kafka Streams Wrapped: 2024 in review](img/hero-image.png)

---

As always, Kafka Streams delivered a solid haul of KIPs over the last year. To celebrate all the hard work that went into these and thank the community members who contributed or participated in a KIP, we thought it would be fun to take a look at what we shipped in 2024. We hope you come away from this with a better sense of what’s been going on in the Kafka Streams community and maybe even get inspired to try out one of these in action.

> **What is a KIP?**
> A KIP is a **K**afka **I**mprovement **P**roposal, which typically defines a set of changes to improve Apache Kafka in some way. It is a mechanism by which the community can agree on larger changes to the project, especially those that add, remove, or modify public interfaces. The KIPs listed here are those that improved Kafka Streams in 2024, and span new features, richer APIs, as well as improvements to stability and performance of Kafka Streams.

## Kafka Streams had a LOT of improvements!

We’ll  start with the full laundry list of all the KIPs from 3.7 - 4.0 (and yeah, we’re kind of cheating since technically 4.0 comes out in January, but that’s just because it got bumped back quite a few times for different reasons. So we’re going to claim it for 2024.) Here is the last year of KIPs that touch on, and hopefully improve, Kafka Streams:

3.7:

- [KIP-925: Rack aware task assignment in Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-925%3A+Rack+aware+task+assignment+in+Kafka+Streams)
- [KIP-954: expand default DSL store configuration to custom types](https://cwiki.apache.org/confluence/display/KAFKA/KIP-954%3A+expand+default+DSL+store+configuration+to+custom+types)
- [KIP-960: Single-key single-timestamp IQv2 for state stores](https://cwiki.apache.org/confluence/display/KAFKA/KIP-960%3A+Support+single-key_single-timestamp+interactive+queries+%28IQv2%29+for+versioned+state+stores)
- [KIP-962: Relax non-null key requirement in Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-962%3A+Relax+non-null+key+requirement+in+Kafka+Streams)
- [KIP-968: Support single-key_multi-timestamp Interactive Queries (IQv2) for Versioned State Stores](https://cwiki.apache.org/confluence/display/KAFKA/KIP-968%3A+Support+single-key_multi-timestamp+interactive+queries+%28IQv2%29+for+versioned+state+stores)
- [KIP-985: Add reverseRange and reverseAll query over kv-store in IQv2](https://cwiki.apache.org/confluence/display/KAFKA/KIP-985%3A+Add+reverseRange+and+reverseAll+query+over+kv-store+in+IQv2)
- [KIP-988: Streams Standby Update Listener](https://cwiki.apache.org/confluence/display/KAFKA/KIP-988%3A+Streams+Standby+Update+Listener)
- [KIP-992: Proposal to introduce IQv2 Query Types: TimestampedKeyQuery and TimestampedRangeQuery](https://cwiki.apache.org/confluence/display/KAFKA/KIP-992%3A+Proposal+to+introduce+IQv2+Query+Types%3A+TimestampedKeyQuery+and+TimestampedRangeQuery)

3.8:

- [KIP-989: Improved StateStore Iterator metrics for detecting leaks](https://cwiki.apache.org/confluence/display/KAFKA/KIP-989%3A+Improved+StateStore+Iterator+metrics+for+detecting+leaks)
- [KIP-924: customizable task assignment for Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-924%3A+customizable+task+assignment+for+Streams)
- [KIP-813: Shareable State Stores](https://cwiki.apache.org/confluence/display/KAFKA/KIP-813%3A+Shareable+State+Stores)

3.9:

- [KIP-1049: Add config log.summary.interval.ms to Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1049%3A+Add+config+log.summary.interval.ms+to+Kafka+Streams)
- [KIP-1033: Add Kafka Streams exception handler for exceptions occurring during processing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing)

4.0:

- [KIP-1112: allow custom processor wrapping](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1112%3A+allow+custom+processor+wrapping)
- [KIP-1087: Removing intermediateTopicsOption from StreamsResetter](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1087%3A+Removing+intermediateTopicsOption+from+StreamsResetter)
- [KIP-1085: Fix leaking *_DOC variables in StreamsConfig](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1085%3A+Fix+leaking+*_DOC+variables+in+StreamsConfig)
- [KIP-1078: Remove Leaking Getter Methods in Joined Helper Class](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1078%3A+Remove+Leaking+Getter+Methods+in+Joined+Helper+Class)
- [KIP-1077: Deprecate `ForeachProcessor` and move to internal package](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1077%3A+Deprecate+%60ForeachProcessor%60+and+move+to+internal+package)
- [KIP-1076: Metrics for client applications KIP-714 extension](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1076%3A++Metrics+for+client+applications+KIP-714+extension)
- [KIP-1070: deprecate MockProcessorContext](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1070%3A+deprecate+MockProcessorContext)
- [KIP-1065: Add "retry" return-option to ProductionExceptionHandler](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=311627309)
- [KIP-1056: Remove `default.` prefix for exception handler StreamsConfig](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1056%3A+Remove+%60default.%60+prefix+for+exception+handler+StreamsConfig)
- [KIP-1106: Add duration based offset reset option for consumer clients](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1106%3A+Add+duration+based+offset+reset+option+for+consumer+clients)
- [KIP-1104: Allow Foreign Key Extraction from Both Key and Value in KTable Joins](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1104%3A+Allow+Foreign+Key+Extraction+from+Both+Key+and+Value+in+KTable+Joins)
- [KIP-1091: Improved Kafka Streams operator metrics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1091%3A+Improved+Kafka+Streams+operator+metrics)

That’s a long list of KIPs, so don’t worry — we won’t be going on all of them in this blog post, and certainly not in painstaking detail. But just remember, if you do feel an urge for all the gory details of a KIP or are curious about something in the list above that we don’t cover, you can get that too! Just click on any of the KIPs in this list to check out the KIP document.

There’s a lot of good stuff in that list though, so it would be a shame not to dive deeper. So we picked out the most interesting and useful KIPs , organized them a bit to tell a coherent (but not  necessarily chronological) story, and then dove into what each KIP means for Kafka Streams and its users.

## API Enhancements

The reason developers love Kafka Streams is because it delivers a simple, general purpose, powerful API in an embedded form factor. The API keeps evolving based on community feedback, and there are two improvements that were merged in 2024 that are worth calling out.

### [KIP-1112: allow custom processor wrapping](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1112%3A+allow+custom+processor+wrapping) (4.0)

The first one we’ll visit today, and one of the KIPs we at Responsive are most excited for, is KIP-1112 which is coming out in 4.0. The idea behind this KIP is simple but powerful: allow the user to inject custom wrappers around any or every processor in their application topology. This includes not just the user’s processors (for example when using the `.process` operator or the Processor API) but also the processors that DSL operators are compiled into!

The potential use cases for this KIP are many, and the only limit is your imagination! Here are a few examples:

- Injecting common logic or logging across all processors, reducing duplicate code and extending access to DSL-made processors
- Provides access to context that isn’t available to DSL operators, such as record headers, timestamps, offsets, and more.
- Turning on a “debug mode” with a single config, which could include extra validation/assertions, peek the processor’s state stores, or log additional details and context
- Implement record tracing to follow the path each record takes through the topology and potentially record custom metrics

### [KIP-954: expand default DSL store configuration to custom types](https://cwiki.apache.org/confluence/display/KAFKA/KIP-954%3A+expand+default+DSL+store+configuration+to+custom+types) (3.7)

Back in 3.2, [KIP-591](https://cwiki.apache.org/confluence/display/KAFKA/KIP-591%3A+Add+Kafka+Streams+config+to+set+default+state+store) introduced a new config called `default.dsl.store`, which allowed you to configure a default state store type for DSL operators. Unfortunately, this KIP was limited to only the built-in store types, RocksDB and In-Memory (and RocksDB is the default anyways!)

In 3.7 we introduced KIP-954 to expand this feature to custom store implementations

This should massively improve the custom store experience, which until now required one to manually plug their stores in to every single stateful operator by passing in a Materialized configured with their custom StoreSupplier.

This method had three significant problems:

First of all, it was simply clunky and awkward to use. Not only is it annoying to repeat this for every stateful operator in your topology, it was easy to forget to do this as an application evolved and new operators were added. Even if the topology remains the same, just knowing which operators you need to plug the store into can be difficult and easy to get wrong if you don’t have intimate knowledge of how the DSL is compiled down to processors and state stores.

For example, where do you put the Materialized for this table-table join?

```java
final KTable<String, String> t1 = builder.table("input1");
final KTable<String, String> t2 = builder.table("input2");

t1.join(t2, (v1, v2) -> v1 + v2)
.toStream()
.to("output");
```

You may think it goes in the `.join` operator, since it has an overload that accepts a Materialized and is the main processing step in this short topology. But it’s actually the two `builder#table` steps that you need to pass it into for the join! The join actually forces the two upstream KTables to be materialized and then uses these stores for the join. If you pass the Materialized in to the `join` instead, not only will the two KTables stores not use your custom store implementation, you will actually end up materializing a third store that isn’t needed and wouldn’t otherwise exist!

Following from that, the second problem is that piggybacking on the Materialized configuration object can have unintended side effects. Since passing in a Materialized automatically forces materialization of stores that could have been optimized away. This leads to creating more state stores than the application actually needs, costing resources and hurting performance.

Third, passing in a StoreSupplier to Materialized also forces the store to become available for IQ, which may not be desired and could even be a security concern.

KIP-954 also filled in some gaps in the original KIP, making the config work for all DSL operators, such as the stream-stream join.

## Task assignment improvements

One of the core features of Kafka Streams is deciding where tasks should run, and moving them around in case of node failures. This is a particularly hard problem for stateful applications, since the assignor needs to factor in where copies of the state for the task already exist when deciding placement. Needless to say, this is a complicated subject and one that is constantly being invested in by the community.

We also had two significant KIPs focused on task assignment which delivered some powerful new functionality and customizability of the Kafka Streams assignor.

### [KIP-925: Rack aware task assignment in Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-925%3A+Rack+aware+task+assignment+in+Kafka+Streams) (3.7)

3.7 gave us KIP-925, which extended the consumer client’s existing rack-aware assignment to Kafka Streams applications. The KIP introduced a complex — and super interesting! — graph algorithm that can optimize a given task assignment to minimize cross-rack traffic, saving our users some of the expensive cross-AZ traffic.

It’s super easy to enable this feature as it just requires setting the [rack.id](http://rack.id) for your brokers and your Streams nodes — the exact same way it works for the plain consumer clients At the same time, it also offers several configuration options for advanced use cases where it may be important to carefully balance the tradeoffs.

If cross-AZ traffic is giving you headaches every month when the CSP bill comes in, you should definitely take advantage of this powerful new feature!

### [KIP-924: customizable task assignment for Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-924%3A+customizable+task+assignment+for+Streams) (3.8)

While the default task assignor in Kafka Streams has many powerful abilities and optimizations, from warmup tasks to rack-awareness, sometimes users find that a particular application or use case/requirement is struggling because of some feature or side effect of this assignor. Others find the assignor generally beneficial, but wish they could make slightly different tradeoffs. Or enforce additional constraints. Or adjust an assignment to adapt to workload imbalances. Or…the possibilities are endless.

Until KIP-924 was a black box with no abilities to be customized beyond the handful of assignment algorithm parameters in StreamsConfig.

For instance, the default assignor — called the HighAvailabilityTaskAssignor (or HATaskAssignor for short) — was ultimately focused entirely on stateful applications which prioritize availability of tasks (as the name suggests). While this assignor was able to significantly reduce the downtime of stateful tasks, it was making some tradeoffs that just didn’t make sense for a large subset of application types and use cases

- For example, the most critical drawbacks are its lack of stickiness/increased task shuffling, which leave the potential for transient-but-long periods of uneven task distribution, and increased resource consumption. This makes capacity planning difficult in some cases.
- It was especially bad for stateless applications, which saw none of the benefits of this assignment strategy yet suffered extensively from its lack of “stickiness” and tasks get migrated between clients on nearly every rebalance causing unnecessary disruptions.
- Optimizing for stateful apps meant that stateless applications got the short end of the stick. They saw none of the benefit of this assignor, but did get stuck with a serious disadvantage: because the assignor can move task

Ultimately, there is no one-size-fits-all when it comes to task assignment, so the community has introduced a way to completely customize assignment to suit your needs through a new `TaskAssignor` interface.

Writing your own assignor is no simple task, and we will write a blog post in the future to help you do it well. But in the mean time, being able to customize task assignment is a major step forward to enabling Kafka Streams to work well for any conceivable workload.

## Monitoring improvements

### [KIP-1033: Add Kafka Streams exception handler for exceptions occurring during processing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing) (3.9)

This next KIP adds a feature that I know I’ll be recommending for every single Kafka Streams application from now on: an exception handler for processing errors!

The processing exception handler is akin to the deserialization or production exception handlers, and has a similar API with options to log and continue or fail depending on the error and record context. Any exceptions thrown while processing a record will be forwarded to this handler, whether it comes from user code or Streams code (eg when accessing state stores). Punctuations will also use this handler to determine the response to exceptions thrown in the #punctuate method

Even if you don’t implement any custom exception handling logic, the handler can help you monitor the health of an application and debug problems quickly when they arise. Since the processing handler is applied during the processing step, it has access to a lot  of additional information that isn’t available to things like the StreamsUncaughtExceptionHandler that have to wait for the error to be thrown all the way up.

If you know your application may experience some kind or recoverable exception, you should definitely look into this KIP!

### [KIP-988: Streams Standby Update Listener](https://cwiki.apache.org/confluence/display/KAFKA/KIP-988%3A+Streams+Standby+Update+Listener) (3.7)

Another simple but effective new monitoring tool comes to us from KIP-988, with the introduction of the standby state listener. You can think of this as the standby-task equivalent of the restore listener that Kafka Streams will invoke at the start, end, and at regular intervals of the restoration process. The API is a bit different since active task restoration and standby task processing have slightly different semantics, but the idea is the same.

### [KIP-1091: Improved Kafka Streams operator metrics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1091%3A+Improved+Kafka+Streams+operator+metrics) (4.0)

KIP-1091 has given us some new metrics which have been sorely needed for a long time. The three new metrics are the `client.state`(ie  KafkaStreams state), `thread.state` (ie StreamThread state), and recording.level. The third one is just the recording level of the metrics itself (eg `INFO` or `DEBUG`), but it’s the first two that are powerful tools in the monitoring arsenal: client and thread state

Sharp-eyed readers will probably be thinking “wait…there already is a metric for the Kafka Streams state!” And that is true — but the existing metrics have often been unusable due to reporting the state as a string, whereas many metrics frameworks require numerical data. So this KIP defines a mapping from each of the KafkaStreams states to an integer which should be possible to work with no matter your setup.

While we already had some version of the `client.state`, the `thread.state` is entirely new and will complement the client state to aid in monitoring and understanding what’s going on within a running (or not RUNNING!) Kafka Streams app. Not only does it let you differentiate threads and determine whether there’s a single thread acting up or all of them, the StreamThread’s states themselves are more granular than the KafkaStreams state.

The `client.state` is great for spotting errors and knowing when the app is up and running, but it doesn’t differentiate between actual rebalancing and state restoration, meaning it will report the state as `REBALANCING` not just during the rebalance but also after, until it has completely finished restoring any state. The StreamThread however will enter the state `PARTITIONS_REVOKED` at the start of a rebalance, and then transition to `PARTTIONS_ASSIGNED` once the rebalance is finished and its tasks have been assigned. This means you can actually see when the thread is rebalancing and when it’s restoring state. Even better, you can see when it has started *another* rebalance while it was still restoring state after the first, and keep track of rebalancing loops  in this way, whereas the `client.state` would just continue to stay in `RESTORING` .

These metrics are a huge step forward in identifying and debugging those dreaded rebalance loops!

### [KIP-989: Improved StateStore Iterator metrics for detecting leaks](https://cwiki.apache.org/confluence/display/KAFKA/KIP-989%3A+Improved+StateStore+Iterator+metrics+for+detecting+leaks) (3.8)

KIP-989 introduces a few new metrics for state stores, more specifically for their iterators If you’ve ever used a range query in a custom processor, you should be familiar with these iterators and hopefully also know that they need to be closed to prevent leaking memory when using RocksDB (although you should always make sure to close them even if the store is a different type!).

The metrics in this KIP make it much easier to spot a leaking iterator, allowing you to immediately spot any potential problems before they become real problems. We recommend adding them to your dashboards if you do range queries in a custom processor.

## Looking forward

We at Responsive are very grateful for the Kafka Streams community. It’s a passionate group of people who provide amazing feedback and make contributions. Thanks to the community’s efforts, Kafka Streams continues to be the #1 embedded stream processor and [continues to be the default choice](https://github.com/responsivedev/awesome-kafka-streams/) for a wide range of companies building mission critical event driven applications. The pace of progress is immense, and we will be doing a follow up blog post on all the major improvements you can look forward to in 2025 and beyond.

We at Responsive are also grateful that we get to play a part in the Kafka Streams story. In 2024, we released an [SDK](https://www.responsive.dev/sdk) that brings remote state, async processing, and more to Kafka Streams, thus giving the community several options for further improving the reliability and scalability of their applications. There is a lot more we plan to do in 2025, and look forward to enjoying the ride with all of you!
