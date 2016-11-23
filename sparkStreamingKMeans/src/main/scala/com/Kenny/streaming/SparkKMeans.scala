package com.Kenny.streaming

/**
  * Created by kzhang on 11/22/16.
  */

import java.io.{ByteArrayInputStream, DataInputStream}

import _root_.kafka.serializer.StringDecoder
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.State
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{sql, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.StatCounter
import org.apache.spark.streaming.{State, StateSpec, Time}
import org.apache.spark.mllib.regression.{LabeledPoint,StreamingLinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.State
import org.apache.spark.sql.hive._

import scala.reflect.runtime._

//import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
//import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.hive.HiveMetastoreCatalog
import scala.collection.JavaConverters._
import scala.concurrent.duration._

object SparkKMeans {
  def main(args: Array[String]) {

    def isNumeric(input: String): Boolean = {
      input.forall(_.isDigit)
    }

    //val df = new SQLContext().sql().toString().toSet.toSeq

    def trackStateFunc(batchTime: Time, key: Double, value: Option[Int], state: State[Long]): Option[(Double, Long)] = {
      val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }

    case class Record(RandomNum: Double, time: Long) {
      def toPair: (Double, Long) = {
        (RandomNum, time)
      }
    }

    /*  def isNumeric (input: Array[String]): Boolean = {
        input.filter(isNumeric(_))
      }*/

    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <batchInterval> is the batch interval which is also the sliding interval
                            |  <windowLength> is duration of window
                            |  <securityProtocol> is the desired Kafka security protocol
        """.stripMargin)
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    //val hbaseConf = HBaseConfiguration.create()

    //val admin = new HBaseAdmin(hbaseConf)

    val Array(brokers, topics, batch, window, securityProtocol) =
      if (args.length > 4) args else args :+ KafkaUtils.securityProtocolDefault
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KennyKafkaStream")
    val ssc = new StreamingContext(sparkConf, Seconds(batch.toLong))
    ssc.checkpoint("/tmp/checkpoint")
    val sc = ssc.sparkContext

    //val hc = new HiveContext(sc)
    //hc.sql("select * from test").collect()

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      KafkaUtils.securityProtocolConfig -> securityProtocol, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    //Format: RandomNum: 79 with time: 1469671243
    /* val numPart = messages.
     println(s"Number of partitions : {$messages}")*/
    val keys = messages.map(_._1)//keys.print() it's null
    val lines = messages.map(_._2)
    //lines.print(1)
    val records = lines.map(_.split(" ")).map((ws : Array[String]) => Record(ws(1).toDouble, ws(4).toLong))
    //records.print(2)
    /*Record(99,1472004745)*/

    val stateSpec = StateSpec.function(trackStateFunc _).numPartitions(2)
    //.window(Seconds(window.toLong),Seconds(batch.toLong))
    val numStream = records.map(r => (r.RandomNum, 1))

    val numStateStream = numStream.mapWithState(stateSpec)
    numStateStream.print() //val tokenInfo = numStream.map(r => printTokenInfo(sparkConf))

    val NumStat = records.map(r => r
      .RandomNum).window(Seconds(window.toLong),Seconds(batch.toLong)).
      foreachRDD(rdd => { val s = rdd.stats; println("Statistics from last " +
      ""+ window + " seconds: " + s)})



    //val timeStat = records.window(Seconds(window.toLong),Seconds(batch.toLong)).map(r => r.time).foreachRDD(rdd => { val s = rdd.stats; println("From: " + s.min + " to: " + s.max)})
    /*reduce((r1:List[Double],r2:List[Double]) => { r1.zip(r2).flatten } )
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow((a:Long,b:Long) => (a + b), Seconds(window.toLong), Seconds(batch.toLong))*/
    //wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    }
}
