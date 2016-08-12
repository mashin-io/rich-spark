Table of contents
=================
* [Overview](#overview)
* [Quick Example](#quick-example)
* [Basic Concepts](#basic-concepts)
  * [Event](#event)
  * [Event Source](#event-source)
    * [Timer Event Source](#timer-event-source)
    * [HDFS Event Source](#hdfs-event-source)
    * [ReactiveX Event Source](#reactivex-event-source)
    * [REST Server Event Source](#rest-server-event-source)
    * [Custom Event Sources](#custom-event-sources)
    * [Operations on Event Sources](#operations-on-event-sources)
  * [Dependency](#dependency)
    * [Event Dependency](#event-dependency)
    * [Tail Dependency](#tail-dependency)
    * [Time Window Dependency](#time-window-dependency)
  * [Bind Logic](#bind-logic)
    * [Cascaded Jobs](#cascaded-jobs)
      * [Dependency Operations](#dependency-operations)
    * [Default Timer](#default-timer)
  * [DStream Output Operations](#dstream-output-operations)
  * [DStream Checkpoint Interval](#dstream-checkpoint-interval)
* [Using the Package](#using-the-package)

<a name="overview"/>
#Overview

The streaming API of Apache Spark assumes an equi-frequent micro-batches model such that streaming data are divided into batches for which jobs are submitted to the Spark engine every fixed amount of time (aka `batchDuration`).

![Spark Streaming Old Model](img/reactive-stream-old-model.png)

All input streams are dealt with in the same way. The same recurrent timer allocates batches for all streams on every batch duration.

![Spark Streaming Old Model](img/reactive-stream-old-model-2.png)

Reactive Spark API is an extension of this model; instead of generating a batch every `batchDuration`, batch generation becomes event based. Spark listens to event sources and generates batches upon events. The equi-frequent micro-batches model becomes equivalent to a timer event source that fires a timer event every `batchDuration`.

![Spark Streaming New Model](img/reactive-stream-new-model-2.png)

This allows a fine grain scheduling of Spark jobs. Different input streams with different data rates, each could have its own event source. Either high frequency input data streams or low frequency input batch data, both kinds could be dealt with by the same API such that the same code could serve streaming and batch data.

![Spark Streaming New Model](img/reactive-stream-new-model.png)

With this model, a batch job could easily be scheduled to run periodically. There would be no need to deploy or configure an external scheduler (like Apache Oozie, Linux Crons, Mesos Chronos ... etc.). This model easily allows jobs with dependencies that span across time, like daily logs transformations concluded by weekly aggregations. This extension also adds a variety of event sources allowing to schedule jobs not only on timely basis.

<a name="quick-example"/>
#Quick Example

Before digging deeper into the details of the reactive API, let's take a look at some quick examples and see how the reactive API looks like in general.

The following example is about recurrent batch jobs that do some processing on log files.

```scala
val conf = new SparkConf().setMaster("local[*]").setAppName("ReactiveSpark")
val ssc = new StreamingContext(conf, Seconds(1))

// dailyTimerStartTime, dailyTimerEndTime, weeklyTimerStartTime
// and weeklyTimerEndTime are defined somewhere else
val dailyTimer = ssc.timer(dailyTimerStartTime, dailyTimerEndTime, Days(1), "DailyTimer")
val weeklyTimer = ssc.timer(weeklyTimerStartTime, weeklyTimerEndTime, Days(7), "WeeklyTimer")

val logs: DStream[String] = ssc.textFileStream("logs")

logs.filter(line => line.contains("WARN") || line.contains("ERROR"))
  .saveAsTextFiles("daily-logs")
  .bind(dailyTimer)
  .tailWindow(windowLength = 7, slideLength = 7, skipLength = 0)
  .filter(line => line.contains("ERROR"))
  .saveAsTextFiles("weekly-logs")
  .bind(weeklyTimer)

ssc.start()
ssc.awaitTermination()
```

First, there is a job that runs on daily basis that reads raw log files and filters log entries with levels `WARN` and `ERROR` saving the filtered output to disk.

```scala
logs.filter(line => line.contains("WARN") || line.contains("ERROR"))
  .saveAsTextFiles("daily-logs")
  .bind(dailyTimer)
```

Then, another job runs on weekly basis, taking the output of the daily job of the last 7 days, filtering log entries with level `ERROR` and saving the output to different path on disk.

```scala
  .tailWindow(windowLength = 7, slideLength = 7, skipLength = 0)
  .filter(line => line.contains("ERROR"))
  .saveAsTextFiles("weekly-logs")
  .bind(weeklyTimer)
```

Note that we have created two event sources, timers: `dailyTimer` that fires every duration of `Days(1)` or one day and `weeklyTimer` that fires every duration of `Days(7)` or one week.

```scala
val dailyTimer = ssc.timer(dailyTimerStartTime, dailyTimerEndTime, Days(1), "DailyTimer")
val weeklyTimer = ssc.timer(weeklyTimerStartTime, weeklyTimerEndTime, Days(7), "WeeklyTimer")
```

To tell Spark what job to execute on what events, we used the method `bind`. Whenever a timer event fires, Spark generates a job for the sequence of transformations before the `bind` and upwards. In order for the weekly job to execute, we need to define the dependency between the daily job and the weekly job and we have done that using the method `tailWindow`. Here, `tailWindow` tells Spark that the weekly job depends on the last 7 outputs from the daily job.

<a name="basic-concepts"/>
#Basic Concepts

<a name="event"/>
##Event

A batch of jobs is generated per event. Each event has:

1. **instanceId:** a universal unique id per context
2. **index:** the index of the event with respect to events from the same event source
2. **time:** a timestamp denoting the time when the event was fired
3. **source:** the source of the event 

Events are ordered naturally with their time however they could also be ordered locally by their index and globally by their instance id. For two events `e1` and `e2` from the same event source, `e1.index` is less than `e2.index` if and only if `e1` has happened before `e2`. For any two events `e1` and `e2`, `e1.instanceId` is less than `e2.instanceId` if and only if `e1` has happened before `e2`.

<a name="event-source"/>
##Event Source

Events are fired by event sources either on regular or irregular basis. Events are for job scheduling purposes and they are not meant to be like the streaming data itself. Events have a respectively lower rate than the streaming data and are fired by a single instance from the event source. Example event sources:

 1. **Timer:** a timer has start and end times, and fires timer events periodically
 2. **File system watcher:** a file system watcher monitors the file system and fires events on file system changes (like the creation of a new file or renaming an existing file ...)
 3. **RESTful web server:** helps triggering a job from an online admin console

Event sources provide listener interfaces so that other parties could get notified for events.

```scala
timer.addListener(new EventListener {
  override def onEvent(event: Event): Unit = {
    println(s"An event has occurred: $event")
  }
})
```

<a name="timer-event-source"/>
###Timer Event Source

A timer event source fires `TimerEvent`'s periodically. It has a `startTime`, an `endTime` and a `period`. Timer event source is used for scheduling timed jobs. In case of driver failure and improper context shutdown, when the context is resumed, the timer event source fires events that were supposed to be fired during the downtime.

```scala
ssc: StreamingContext = ...

val startTime = ZonedDateTime.now
val endTime = startTime.plusWeeks(4)
val period = Days(1)
val name = "daily4WeeksTimer"

val timer = ssc.timer(startTime, endTime, period, name) 
```

<a name="hdfs-event-source"/>
###HDFS Event Source

The HDFS event source is a watcher for the HDFS filesystem. It monitors the given HDFS path for changes and fires events accordingly. The HDFS event source is based on the [HDFS inotify API](https://issues.apache.org/jira/browse/HDFS-6634). In case of driver failure and when the context is resumed from a checkpoint, the HDFS event source remembers where it left off and generates events that were supposed to be fired during the downtime.

```scala
val ssc: StreamingContext = ...

val hdfsURI = "hdfs://localhost:8020"
val pathToWatch = "/user/test/dir/*"
val name = "hdfsWatcher"

// Create an HDFS watcher that fires close events
val hdfsWatcher = ssc.hdfsWatcher(hdfsURI, pathToWatch, name)
  .filter { case e: HDFSEvent => e.getEventType == EventType.CLOSE }

// When a file is closed, read it, then print its content
val textFileStream = ssc.textFileStream(hdfsURI + pathToWatch)
  .print()
  .bind(hdfsWatcher)

ssc.start()
ssc.awaitTermination()
```

The HDFS event source fires different event types that are instances of `HDFSEvent` as well. Available events are:

 Event | Description
-------|------------
`HDFSCloseEvent` | Fired when a file is closed after append or create.
`HDFSCreateEvent` | Fired when a new file is created (including overwrite).
`HDFSMetadataUpdateEvent` | Fired when there is an update to directory or file (none of the metadata tracked here applies to symlinks) that is not associated with another inotify event. The tracked metadata includes atime/mtime, replication, owner/group, permissions, ACLs, and XAttributes. Fields not relevant to the metadataType of the MetadataUpdateEvent will be null or will have their default values.
`HDFSRenameEvent` | Fired when a file, directory, or symlink is renamed.
`HDFSAppendEvent` | Fired when an existing file is opened for append.
`HDFSUnlinkEvent` | Fired when a file, directory, or symlink is deleted.

<a name="reactivex-event-source"/>
###ReactiveX Event Source

[ReactiveX](http://reactivex.io/intro.html) is a library for composing asynchronous and event-based programs by using observable sequences. It extends the observer pattern to support sequences of events and adds operators that allow you to compose sequences together in a declarative functional style. ReactiveX provides [a rich set of operators](http://reactivex.io/documentation/operators.html) with which you can filter, select, transform, combine, and compose Observables.

It is possible to construct an event source from an `Observable` and to convert any event source to an `Observable` using the methods `StreamingContext.rxEventSource` and `EventSource.toObservable`. Using the rich set of operators of [RxScala](http://reactivex.io/rxscala/), it is possible to handle complex usecases and to carry out [operations on event sources](#operations-on-event-sources).

```scala
val sc: StreamingContext = ...

val timer = sc.timer(start, end, period, "timer")

// Convert an event source to an observable
val observable = timer.toObservable

// Perform some transformations using the Observable API
val delayedTimer = observable.delay(10 seconds)

// Convert an observable to an event source
val rxTimer = sc.rxEventSource(delayedTimer, "rxTimer")
```

<a name="rest-server-event-source"/>
###REST Server Event Source

The REST server event source is used for triggering jobs based on HTTP API calls. A valid usecase is to trigger jobs from a web admin console or from command line (e.g. using `curl`). The server implementation is based on [Spark - a micro web framework](http://sparkjava.com/).

```scala
val ssc: StreamingContext = ...

// Define a REST server that binds to address localhost:4140
val restServer = ssc.restServer(host = "localhost", port = "4140", name = "restServer")
  .get(path = "/job1") // can receive GET request on http://localhost:4140/job1
  .get(path = "/job2") // can receive GET request on http://localhost:4140/job2
  
val job1Trigger = restServer.filter {
    case restEvent: RESTEvent => restEvent.request.pathInfo.equals("/job1")
  }
  
val job2Trigger = restServer.filter {
    case restEvent: RESTEvent => restEvent.request.pathInfo.equals("/job2")
  }

val stream1 = ...
stream1.bind(job1Trigger)

val stream2 = ...
stream2.bind(job2Trigger)
```

```bash
# From command line

# Trigger job1
curl http://localhost:4140/job1

# Trigger job2
curl http://localhost:4140/job2
```

<a name="custom-event-sources"/>
###Custom Event Sources

It is possible to implement custom event sources. Custom event sources must extend the abstract class `EventSource` and override the following five methods:

```scala
def start()
def restart()
def stop()
def between(from: Time, to: Time): Seq[Event] = Seq.empty
def toProduct: Product
```

The `start` method is called once when the streaming context is started. It should contain the logic that initializes and starts the event source. The `restart` method is called once instead of `start` when the streaming context is restarted from a checkpoint. It might contain the same code as `start` however it should consider, for example, setting events index counter to a suitable starting point. The `stop` method is called once when the streaming context is stopped. The `toProduct` method is used to convert the event source to a tuple that contains the mandatory parameters that distinguish instances from this event source. For example, the `TimerEventSource` returns the tuple `(startTime, endTime, period, name)`. The optional method `between` is used to generate events in bulk that fall within a time period `[from, to]` inclusively. This is useful for recovering from failures if the streaming context had some down time.

The custom event source must generate events in a different thread than the thread of the caller of the `start` method. When an event occurs, the events generator thread must call the `post` method of the event source in order to notify the listeners with the new event.

Since the custom event source has its own kind of events, new event types must be implemented by extending the abstract class `Event`. The following basic attributes should be set: *eventSource*, *index* and , optionally, *time*.

<a name="operations-on-event-sources"/>
###Operations on Event Sources

Events could get complex hence the Reactive API provides a set of functional operations that could be performed on event sources to handle more complex scenarios. Operations like `map`, `filter` ... etc. The set of operations that are currently implemented are very limited however it is possible to carry out more complex transformations using the `Observable` API of ReactiveX as illustrated in the [ReactiveX Event Source](#reactivex-event-source) subsection.

<a name="dependency"/>
##Dependency

In a sequence of transformations, a dependency defines what RDDs to generate (or pass) from the parent stream to the dependent stream given an event.

<a name="event-dependency"/>
###Event Dependency

A stream directly asks the parent stream for the RDD corresponding to the given event.

![Event Dependency](img/reactive-stream-event-dependency.png)

<a name="tail-dependency"/>
###Tail Dependency

A stream depends on a window of RDDs from the parent stream. The boundaries of the window are computed by counting events occurred before and at the same as the given event. There are three parameters that determine the window boundaries:

 - *skip length:* determines how many RDDs to skip corresponding to events occurred before or at the same time as the given event
 - *window length:* determines how many RDDs to include in the window corresponding to events occurred just before the skipped events
 - *slide length:* determines the number of events after which the window is computed

![Tail Dependency](img/reactive-stream-tail-dependency.png)

It worth noting that, for the same parameters, windows could have different length in terms of time however the same length in terms of number of events. That could happen when events fire at irregular basis.

Also, a *tail dependency* with *skip length = 0*, *window length = 1* and *slide length = 1* is different than an *event dependency*. The *tail dependency* passes the RDD generated on the last event that had occurred before or at the same time as the given event however the *event dependency* passes the RDD generated at the given event itself. In case the parent stream has no RDDs generated at the given event, *event dependency* passes no RDDs however *tail dependency* does.

<a name="time-window-dependency"/>
###Time Window Dependency

A stream depends on a time window of RDDs from the parent stream. There is an incurred latency before a window is fired as a window is triggered upon the arrival of the first event after its end boundary. The window is defined by two parameters:

 - *window duration:* the length of the window in time units
 - *slide duration:* determines how much at least subsequent windows are apart in time

![Time Window Dependency](img/reactive-stream-time-window-dependency.png)

It worth noting that, for the same parameters, windows could have different length in terms of number of events however the same length in terms of time duration. That happens in case events fire at irregular basis. Also, two subsequent windows could be more than one slide duration apart in time. That might happen when events fire with a lower rate than the window slide rate.

<a name="bind-logic"/>
##Bind Logic

Event sources are connected to streams via the `bind` operator. For any sequence of operations, the event source is bound to only the stream on which `bind` is called.

When an event is fired, a job is generated for each stream that is bound to the corresponding event source regardless from the type of the operation that is bound to the event source, whether it is a lazy transformation or an output operation. In case it is a lazy transformation (like a `map`), an empty job is generated causing the underlying RDDs to be materialized.

Events start on the streams bound to their event sources and propagate upwards the sequence of stream transformations generating the corresponding sequence of RDDs transformations. A stream reacts to an event if the stream is bound to the corresponding event source or it is not bound to any event source at all. Otherwise, event propagation stops. Unless a suitable dependency is defined, no jobs are generated for that specific sequence of stream transformations.

A job contains a sequence of transformations ending at a stream bound to an event source and starting from either the first input stream or from the last stream bound to an event source.

![Job Generation](img/reactive-stream-job-generation.png)

<a name="cascaded-jobs"/>
###Cascaded Jobs

In order to cascade a sequence of jobs that are bound to different event sources, a suitable dependency between any two cascaded jobs should be defined. In the following figure, the `Mapped Stream2` defines an *event dependency* on the `Output Stream`. When `Timer2` fires an event, it propagates upwards the dependencies. When the `Mapped Stream2` asks the `Output Stream` for RDDs through the *event dependency*, the `Output Stream` finds that it is not bound to `Timer2` and ignores the event, hence it passes no RDD, hence no job is generated.

![Job Generation - Ignored Job](img/reactive-stream-job-generation-ignored.png)

In order to correct this situation, a suitable dependency should be defined at the jobs boundaries. A suitable dependency could be a *tail dependency* with *skip length = 0*, *window length = 1 and slide length = 1*. In that case, the added `Tail Window Stream` would ask the parent `Output Stream` through a *tail dependency* for the RDD that had been generated at event that had occurred on the same time as or before the given event of `Timer2`. If there is any, a job would be generated.

![Job Generation - Ignored Job Fixed](img/reactive-stream-job-generation-ignored-fixed.png)

<a name="dependency-operations"/>
####Dependency Operations

The following table summarizes the operations used to set the dependency among stream transformations.

Operation | Meaning
----------|---------
**tailWindow**(*windowLength, slideLength, skipLength*) | Return a new DStream in which each RDD contains all the elements seen in a sliding window of events over this DStream. For example, a window with *windowLength* = 4, *slideLength* = 3 and *skipLength* = 2, will start a new window every 3 events skipping RDDs of the latest 2 events and adding RDDs of the former 4 events directly before the skipped 2 events. *windowLength* is the width of the window as the number of subsequent events to contain in the window. *slideLength* is the sliding length of the window (i.e., the number of events after which the new DStream will generate RDDs). *skipLength* is the skip length of the window as the number of the latest subsequent events to skip and not to add to the window.
**timeWindow**(*windowDuration*) | Return a new DStream in which each RDD contains all the elements seen in a sliding window of time over this DStream. A window is generated at the arrival of the first event after the end boundary. Hence, a latency is incurred waiting for the first event after the end boundary. The sliding duration is equal to the window duration. *windowDuration* is the width of the window.
**timeWindow**(*windowDuration, slideDuration*) | Return a new DStream in which each RDD contains all the elements seen in a sliding window of time over this DStream. A window is generated at the arrival of the first event after the end boundary. Hence, a latency is incurred waiting for the first event after the end boundary. *windowDuration* is the width of the window. *slideDuration* is the sliding interval of the window (i.e., the interval after which the new DStream will generate RDDs).

<a name="default-timer"/>
###Default Timer

If an output operation is not bound to any event source, Spark binds it to a default timer event source when the streaming context is started. The default timer starts immediately when the context starts and has an infinite end time. The default timer has a period that equals the `batchDuration` with which the streaming context is configured. This is to stay compatible with the legacy Spark streaming API.

<a name="dstream-output-operations"/>
##DStream Output Operations

Output operations of streams like `print`, `saveAs*Files` and `foreachRDD` in the legacy streaming API return nothing. Those operations are supposed to trigger the actual computations on the data pushing the output to external systems. However in the reactive context, in order to enable job cascading, output operations now return a `DStream`. The returned stream performs an identity `map` on the RDDs of the parent stream hence, effectively, the returned stream is the same as the parent stream.

<a name="dstream-checkpoint-interval"/>
##DStream Checkpoint Interval

A stream could be set to checkpoint data every now and then. The checkpoint interval could be set in two ways:

- *duration:* `dstream.checkpoint(checkpointDuration)` sets the stream to checkpoint after at least `checkpointDuration` has passed since the last checkpoint
- *count:* `dstream.checkpoint(checkpointCount)` sets the stream to checkpoint after at least a number of events equal to `checkpointCount` has occurred since the last checkpoint

In case both `checkpointCount` and `checkpointDuration` are set, both events duration and count are considered and a checkpoint happens whenever any criterion is satisfied.

<a name="using-the-package"/>
#Using the Package

In order to setup Maven and your project POM file to use the package, add the following repository to the list of repositories:

```xml
<repository>
  <id>mashin</id>
  <url>https://github.com/mashin-io/mvn/raw/master/</url>
</repository>
```

Also, use the following dependency:

```xml
<dependency>
  <groupId>io.mashin.rich-spark</groupId>
  <artifactId>rich-spark-streaming_2.11</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```