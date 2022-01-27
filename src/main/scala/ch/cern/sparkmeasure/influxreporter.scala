package ch.cern.sparkmeasure

import com.influxdb.LogLevel
import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.WriteApiBlocking
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.exceptions.InfluxException
import org.slf4j.LoggerFactory

import java.util
import java.util.Map


class InfluxDBReporter (influxdbToken : String,influxdbBucket : String ,influxdbOrg : String, influxdbUrl : String) {

  private val token = influxdbToken
  private val bucket = influxdbBucket
  private val org = influxdbOrg
  private val url = influxdbUrl

//  private val bucket = "sparkometer"
//  private val org = "bigspark.dev"
//  private val url = "https://eu-central-1-1.aws.cloud2.influxdata.com"

  private var influxDB : InfluxDBClient = ensureInfluxDBCon();

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  def post(point : Point, timeStamp : AnyRef, precision : WritePrecision): Unit = {
    ensureInfluxDBCon()
    val writeApi = this.influxDB.getWriteApiBlocking
    try {
//      val point = Point.measurement(measurement)
//        .addTag("appId", appId).addFields(fields)
//        .time(startTime, WritePrecision.MS)
      writeApi.writePoint(this.bucket, this.org, point)
    } catch {
      case ex: InfluxException =>
        System.out.println("Exception while writing to InfluxDB cloud : " + ex)
    }
  }

  def close(): Unit = {
    this.influxDB.close()
    this.influxDB = null
  }

  private def ensureInfluxDBCon(): InfluxDBClient = {
    if (this.influxDB != null) return influxDB
    System.out.println("Trying to connect InfluxDB using url=" + this.url + ", bucket=" + this.bucket + ", org=" + this.org)
    this.influxDB = InfluxDBClientFactory.create(this.url, this.token.toCharArray)
    this.influxDB.setLogLevel(LogLevel.BASIC)
    this.influxDB.enableGzip
    influxDB
  }
}
