package com.github.mrodriguez.sparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming extends App {

  val conf = new SparkConf()
  //For local testing
  //conf.setMaster("local[*]")
  conf.setAppName("StreamTest")
  //conf.set("spark.streaming.kafka.maxRatePerPartition", "500")

  val kafkaParams = Map[String, Object](

    "bootstrap.servers" -> args(0),
    //"bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> args(1),
    "auto.offset.reset" -> "earliest",

    //kafka client test values
    //"max.poll.records" -> "100",
    //"max.partition.fetch.bytes" -> "524288",
    //"request.timeout.ms" -> "40000",
    //"session.timeout.ms" -> "35500",
    //"fetch.max.wait.ms" -> "500",

    "enable.auto.commit" -> "false"
  )

  val topics = Array(args(2))

  //Topic hardcoded
  //val topics = Array("test1")

  //Batch duration hardcoded
  //val streamingContext = new StreamingContext(conf, Seconds(5))

  val streamingContext = new StreamingContext(conf, Seconds(Integer.parseInt(args(3))))
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value)).print()


  stream.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition { iter =>
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    }
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

  // Start the context
  streamingContext.start()
  streamingContext.awaitTermination()

}
