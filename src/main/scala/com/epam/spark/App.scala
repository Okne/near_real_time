package com.epam.spark

import java.util.Date

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import eu.bitwalker.useragentutils.UserAgent
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark._

import scala.collection.mutable
import scalaz._
import scalaz.Scalaz._

object App {

  case class CityInfo(id: String, name: String, stateId: String, area: String, dens: String, location: String)
  case class AdExchange(id: String, name: String, desc: String)
  case class StateInfo(id: String, name: String, population: String, gsp: String)
  case class LogInfo(id: String, name: String)
  case class TagInfo(id: String, keywordValues: String, keywordStatus: String, pricingType: String, keywordMatchType: String, destinationUrl: String)

  case class Click(bid_id: String, timestmp: String, ipinyou_id: String, user_agent: String, ip: String, region: String,
                   city: String, ad_exchange: String, domain: String, url: String, anonymous_url_id: String, ad_slot_id: String,
                   ad_slot_width: Int, ad_slot_height: Int, ad_slot_visibility: Int, ad_slot_format: Int, paying_price: Double,
                   creative_id: String, bidding_price: Double, advertiser_id: String, user_tags: String, stream_id: String)

  case class ClickInfo(bid_id: String, timestmp: Long, ipinyou_id: String, user_agent: Map[String, String], ip: String, region_info: Map[String, Any],
                       city_info: Map[String, Any], ad_exchange_info: Map[String, Any], domain: String,
                       url: String, anonymous_url_id: String, ad_slot_id: String, ad_slot_width: Int, ad_slot_height: Int, ad_slot_visibility: Int,
                       ad_slot_format: Int, paying_price: Double, creative_id: String, bidding_price: Double, advertiser_id: String,
                       user_tags: Map[String, Any], stream: Map[String, Any])

  val SESSION_TIMEOUT = 30 * 60 * 1000
  // timeout in milliseconds
  val BATCH_TIME_IN_SEC = 1;

  val DICT_SEPARATOR = "\t"
  val KW_SEPARATOR = ","

  val TAGS_FILE_PATH = "hdfs://sandbox.hortonworks.com:8020/mr-data/dic/user.profile.tags.us.txt"
  val CITIES_FILE_PATH = "hdfs://sandbox.hortonworks.com:8020/mr-data/dic/city.us.txt"
  val LOG_TYPES_FILE_PATH = "hdfs://sandbox.hortonworks.com:8020/mr-data/dic/log.type.txt"
  val STATES_FILE_PATH = "hdfs://sandbox.hortonworks.com:8020/mr-data/dic/states.us.txt"
  val AD_EXCHANGE_FILE_PATH = "hdfs://sandbox.hortonworks.com:8020/mr-data/dic/ad.exchange.txt"

  val CHECKPOINT_DIR_PATH = "hdfs://sandbox.hortonworks.com:8020/tmp/checkpoint"

  val DEFAULT_MAP_VALUE = List.empty[String]

  val DATE_FORMAT = new java.text.SimpleDateFormat("yyyyMMddhhmmssSSS")

  val UNKNOWN_STATE_OBJ = StateInfo("", "unknown", "0", "")
  val UNKNOWN_CITY_OBJ = CityInfo("", "unknonw", "", "0.0", "0.0", "0.0,0.0")
  val UNKNOWN_AD_EXCHANGE = AdExchange("", "unknown", "")
  val UNKNOWN_LOG_OBJ = LogInfo("", "unknown")
  val UNKNOWN_TAG_OBJ = TagInfo("", "", "", "", "", "")


  def main(args: Array[String]): Unit = {
    //read passed params
    val appName = args(0)
    val master = args(1)
    val topics = args(2)
    val brokers = args(3)

    val config = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.cassandra.connection.host", "192.168.56.1")
      .set("es.nodes", "192.168.56.1")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //.set("es.index.auto.create", "true")
      //.set("es.resource", "hw2/clicks")
      //.set("spark.cleaner.ttl", "100000")

    val sc = new SparkContext(config)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(CHECKPOINT_DIR_PATH)

    //create broadcast variable for dictionaries
    val tagsMap = loadDictToBroadcastVar(sc,
      TAGS_FILE_PATH,
      line => !line.startsWith("ID"),
      l => TagInfo(l(0), l(1), l(2), l(3), l(4), l(5)))
    val citiesMap = loadDictToBroadcastVar(sc,
      CITIES_FILE_PATH,
      line => !line.startsWith("Id"), l => CityInfo(l(0), l(1), l(2), l(3), l(4), List(l(6), l(7)).mkString(",")))
    val logTypesMap = loadDictToBroadcastVar(sc, LOG_TYPES_FILE_PATH, line => !line.startsWith("Stream"), l => LogInfo(l(0), l(1)))
    val statesMap = loadDictToBroadcastVar(sc, STATES_FILE_PATH, line => !line.startsWith("Id"), l => StateInfo(l(0), l(1), l(2), l(3)))
    val adExchangeMap = loadDictToBroadcastVar(sc, AD_EXCHANGE_FILE_PATH, line => !line.startsWith("Id"), l => AdExchange(l(0), l(1), l(2)))

    //read data from kafka with new approach (no receivers)
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {

      val clicks = rdd.map[(String, String)](msg => {
        val uuid = java.util.UUID.randomUUID.toString
        (uuid, msg._2)
      })

      val merged_clicks = rdd.map(msg => {
        val splits = msg._2.split("\t")

        val state_info = statesMap.value.getOrElse(splits(5), UNKNOWN_STATE_OBJ).asInstanceOf[StateInfo]
        val city_info = citiesMap.value.getOrElse(splits(6), UNKNOWN_CITY_OBJ).asInstanceOf[CityInfo]
        val ad_exchange_info = adExchangeMap.value.getOrElse(splits(7), UNKNOWN_AD_EXCHANGE).asInstanceOf[AdExchange]
        val tags = tagsMap.value.getOrElse(splits(20), UNKNOWN_TAG_OBJ).asInstanceOf[TagInfo]
        val stream = logTypesMap.value.getOrElse(splits(21), UNKNOWN_LOG_OBJ).asInstanceOf[LogInfo]
        val ua = UserAgent.parseUserAgentString(splits(3))
        val uaMap = Map[String, String]("browser" -> ua.getBrowser.getGroup.getName, "os" -> ua.getOperatingSystem.getName,
          "device_type" -> ua.getOperatingSystem.getDeviceType.getName)

        val time = DATE_FORMAT.parse(splits(1)).getTime
        ClickInfo(splits(0), time, splits(2), uaMap, splits(4), ccToMap(state_info),
          ccToMap(city_info), ccToMap(ad_exchange_info), splits(8), splits(9), splits(10),
          splits(11), splits(12).toInt, splits(13).toInt, splits(14).toInt, splits(15).toInt, splits(16).toDouble,
          splits(17), splits(18).toDouble, splits(19), ccToMap(tags), ccToMap(stream))
      })

      //save to hot row cache with 30 min TTL
      //clicks.saveToCassandra("spark_hw_2", "hot_logs_table", writeConf = WriteConf(ttl = TTLOption.constant(1800)))
      //save merged log data to cassandra
      //merged_clicks.saveToCassandra("spark_hw_2", "dwh_logs_table")

      //save merged log data to elasticsearch index
      merged_clicks.saveToEs("hw2/clicks")
    })

    val ipinyouIdsInfo = messages.map[(String, (Long, Long, Click))](message => {
      val splits = message._2.split("\t")
      val click = Click(splits(0), splits(1), splits(2), splits(3), splits(4), splits(5), splits(6), splits(7), splits(8), splits(9),
        splits(10), splits(11), splits(12).toInt, splits(13).toInt, splits(14).toInt, splits(15).toInt, splits(16).toDouble,
        splits(17), splits(18).toDouble, splits(19), splits(20), splits(21))
      val currentDate = new Date()
      (click.ipinyou_id, (currentDate.getTime, currentDate.getTime, click))
    })

    val latestSessionInfo = ipinyouIdsInfo.map[(String, (Long, Long, Map[String, Int]))](a => {
      val tagsInfo = tagsMap.value.getOrElse(a._2._3.user_tags, UNKNOWN_TAG_OBJ)
      val tags = tagsInfo.asInstanceOf[TagInfo].keywordValues.split(KW_SEPARATOR)

      val tagsToCountMap = mutable.Map.empty[String, Int].withDefaultValue(0)
      for (rawWord <- tags) {
        val word = rawWord.toLowerCase
        tagsToCountMap(word) += 1
      }
      (a._1, (a._2._1, a._2._2, tagsToCountMap.toMap))
    }).reduceByKey((a, b) => {
      (Math.min(a._1, b._1), Math.max(a._2, b._2), a._3 |+| b._3)
    }).updateStateByKey(updateStateBySession)

    //filter out expired sessions
    val activeSessions = latestSessionInfo.filter(s => System.currentTimeMillis() - s._2._2 < SESSION_TIMEOUT).map { case (a, b) => (a, b._1, b._2, b._3) }

    activeSessions.foreachRDD(rdd => {
      rdd.saveToCassandra("spark_hw_2", "session_log_tokens", SomeColumns("session_id" as "_1", "start" as "_2", "end" as "_3", "tags" as "_4"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def loadDictToBroadcastVar(context: SparkContext, path: String, filter: String => Boolean, objCreator: List[String] => AnyRef): Broadcast[Map[String, AnyRef]] = {
    val dict = context.textFile(path)
    val filteredDict = dict.filter(filter)
    val dictMap = filteredDict.map(line => {
      val splits = line.split(DICT_SEPARATOR)
      (splits(0), objCreator(splits.toList))
    }).collectAsMap().toMap
    context.broadcast(dictMap)
  }

  def ccToMap(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
    }

  def updateStateBySession(prevState: Seq[(Long, Long, Map[String, Int])],
                           newValue: Option[(Long, Long, Map[String, Int], Boolean)]): Option[(Long, Long, Map[String, Int], Boolean)] = {
    var res: Option[(Long, Long, Map[String, Int], Boolean)] = null

    if (prevState.size == 0) {
      if (System.currentTimeMillis() - newValue.get._2 < SESSION_TIMEOUT + BATCH_TIME_IN_SEC * 1000) {
        res = None
      } else {
        if (newValue.get._4 == false) {
          res = newValue
        } else {
          res = Some((newValue.get._1, newValue.get._2, newValue.get._3, false))
        }
      }
    }

    prevState.foreach(s => {
      if (newValue.isEmpty) {
        res = Some((s._1, s._2, s._3, true))
      } else {
        if (s._1 - newValue.get._1 < SESSION_TIMEOUT) {
          res = Some((
            Math.min(s._1, newValue.get._1),
            Math.max(s._2, newValue.get._2),
            s._3 |+| newValue.get._3,
            false
            ))
        } else {
          res = Some((s._1, s._2, newValue.get._3, true))
        }
      }
    })

    res
  }
}


