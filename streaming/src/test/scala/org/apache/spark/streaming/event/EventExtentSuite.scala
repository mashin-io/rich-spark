package org.apache.spark.streaming.event

import scala.collection.{SortedSet, mutable}

import org.apache.spark.streaming._

class EventExtentSuite extends TestSuiteBase {

  private val timer = new TimerEventSource(null, Time(0), Time(1), Seconds(1), "dummy-timer")

  private implicit def toSortedSet(set: Set[TimerEvent]): SortedSet[Event] = {
    new mutable.TreeSet[Event]()(Event.ordering) ++ set
  }

  test("copy") {
    val r1 = new MaxEventExtent
    r1.set(1)
    r1.set(Seconds(1))

    val r2 = r1.copy

    assert(r1.leaves.forall(l1 => r2.leaves.forall(l2 => l1.hashCode() != l2.hashCode())))

    val events = (1 to 10).map(i => TimerEvent(timer, Time(i * 1000), i)).toSet
    assert(r1.evalCount(events) == r2.evalCount(events))
    assert(r1.evalDuration(events) == r2.evalDuration(events))

    assert(!(new DurationExtentNode(Milliseconds(500)).hashCode() ===
      new DurationExtentNode(Milliseconds(500)).hashCode()))
  }

  test("set") {
    val r = new MaxEventExtent
    var expectedNodes = 0
    def set(
        count: Option[Int] = None,
        duration: Option[Duration] = None,
        shouldSet: Boolean): Unit = {
      if (shouldSet) expectedNodes += 1
      assert(count.map(r.set).getOrElse(duration.map(r.set).get) == shouldSet)
      assert(r.leaves.length == expectedNodes && r.roots.length == expectedNodes)
    }
    set(count = Some(1), shouldSet = true)
    set(count = Some(1), shouldSet = false)
    set(count = Some(3), shouldSet = true)
    set(count = Some(2), shouldSet = false)
    set(duration = Some(Seconds(1)), shouldSet = true)
    set(duration = Some(Seconds(1)), shouldSet = false)
    set(duration = Some(Seconds(3)), shouldSet = true)
    set(duration = Some(Seconds(2)), shouldSet = false)
  }

  test("eval - relative") {
    def eval(r: MaxEventExtent) {
      val events = (1 to 10).map(i => TimerEvent(timer, Time(i * 1000), i)).toSet
      val r2 = new MaxEventExtent
      r2.set(Seconds(7))
      assert(r.evalCount(events) === r2.evalCount(events))
      assert(r.evalDuration(events) === r2.evalDuration(events))
    }

    var r = new MaxEventExtent
    r.set(Seconds(3))
    r += 4
    eval(r)

    r = new MaxEventExtent
    r.set(2)
    r += Seconds(2)
    r += 1
    r += Seconds(2)
    eval(r)

    r = new MaxEventExtent
    r.set(Seconds(3))
    r.set(r + 4)
    eval(r)
  }

  test("eval") {
    val events: SortedSet[Event] = Set(
      TimerEvent(timer, Time(2000), 0),
      TimerEvent(timer, Time(3000), 1),
      TimerEvent(timer, Time(4500), 2),
      TimerEvent(timer, Time(6000), 3),
      TimerEvent(timer, Time(8500), 4))

    def testCase(
        expectedCount: Int,
        expectedDuration: Duration,
        block: MaxEventExtent => MaxEventExtent) {
      var r = new MaxEventExtent
      r = block(r)
      println(s"Extent $r =>")
      println(s"\tcovered events = ${events.takeRight(r.evalCount(events))
          .map(_.index).mkString("{", ", " , "}")}")
      println(s"\tevalCount = ${r.evalCount(events)}")
      println(s"\tevalDuration = ${r.evalDuration(events)}")
      assert(r.evalCount(events) == expectedCount)
      assert(r.evalDuration(events) == expectedDuration)
    }

    testCase(1, Duration(1000), {r => r.set(Duration(1000)); r})
    testCase(1, Duration(1500), {r => r.set(Duration(1500)); r})
    testCase(1, Duration(2000), {r => r.set(Duration(2000)); r})
    testCase(1, Duration(2500), {r => r.set(Duration(2500)); r})
    testCase(2, Duration(3000), {r => r.set(Duration(3000)); r})
    testCase(2, Duration(3500), {r => r.set(Duration(3500)); r})
    testCase(2, Duration(4000), {r => r.set(Duration(4000)); r})
    testCase(3, Duration(4500), {r => r.set(Duration(4500)); r})
    testCase(3, Duration(5000), {r => r.set(Duration(5000)); r})
    testCase(3, Duration(5500), {r => r.set(Duration(5500)); r})
    testCase(4, Duration(6000), {r => r.set(Duration(6000)); r})
    testCase(4, Duration(6500), {r => r.set(Duration(6500)); r})
    testCase(5, Duration(6500), {r => r.set(Duration(7000)); r})
    testCase(5, Duration(6500), {r => r.set(Duration(7500)); r})

    testCase(1, Duration(2500), {r => r.set(1); r})
    testCase(2, Duration(4000), {r => r.set(2); r})
    testCase(3, Duration(5500), {r => r.set(3); r})
    testCase(4, Duration(6500), {r => r.set(4); r})
    testCase(5, Duration(6500), {r => r.set(5); r})
    testCase(5, Duration(6500), {r => r.set(6); r})

    testCase(4, Duration(6000), {r => r.set(2); r + Duration(2000)})
  }

}
