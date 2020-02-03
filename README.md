This package adds more to Apache Spark. Currently, there are two sub packages, **main** and **streaming**.

## Rich Spark Main

The **main** sub package provides minor API extensions like:

- `rdd.scanLeft` and `rdd.scanRight`
- `sc.httpRDD` for creating RDDs from REST API calls
- `ParallelSGD` is a parallelized version of mini-batch stochastic gradient descent (see [SPARK-14880](https://issues.apache.org/jira/browse/SPARK-14880))

```scala
val sc: SparkContext = ...

val serverIP = ...
val serverPort = ...

val reqFactory: Int => HttpRequest = { i =>
  val pageIndex = i + 1
  new HttpGet(s"http://$serverIP:$serverPort/rdd?page=$pageIndex")
}

val resHandler: (Int, HttpResponse) => Iterator[String] = { (i, httpResponse) =>
  val reader = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
  val line = reader.readLine()
  reader.close()
  line.split(",").iterator
}

val numPages = ...
val rdd = sc.httpRDD(reqFactory, resHandler, numPages).cache
```

## Rich Spark Streaming (AKA Reactive Spark)

The **streaming** package is an extension of the [Spark Streaming API](http://spark.apache.org/docs/2.0.0/streaming-programming-guide.html) to allow for built-in scheduling of Spark jobs (both batch and streaming jobs). Instead of deploying and configuring a scheduling service (e.g. Apache Oozie, Mesos Chronos, Linux Crons ...), this extension allows scheduling Spark jobs from within the job code making the scheduling semantics part of the job semantics.

It's not only possible to schedule jobs on timely basis but also based on events from a various set of event sources like filesystem events and REST API calls from a web admin console. Moreover, this extension integrates with [ReactiveX](http://reactivex.io/intro.html) enabling scheduling on complex events.

For more information, please read the [docs](https://github.com/mashin-io/rich-spark/blob/master/docs/reactive-spark-doc.md).

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

## Linking

In order to setup Maven and your project POM file to use the package, add the following repository to the list of repositories:

```xml
<repository>
  <id>mashin</id>
  <url>https://github.com/mashin-io/mvn/raw/master/</url>
</repository>
```

For **main** package, use the following dependency:

```xml
<dependency>
  <groupId>io.mashin.rich-spark</groupId>
  <artifactId>rich-spark-main_2.11</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

For **streaming** package, use the following dependency:

```xml
<dependency>
  <groupId>io.mashin.rich-spark</groupId>
  <artifactId>rich-spark-streaming_2.11</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```
