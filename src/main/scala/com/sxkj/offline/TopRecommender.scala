package com.sxkj.offline

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TopRecommender {

  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val spark = SparkSession
      .builder()
      .appName("Spark TopRecommender to ES")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val confFilePath = "psyduck.properties"
    val props = new Properties()
    props.load(new FileInputStream(confFilePath))

    val dfTop10 = spark.sql(
      """select *,(t1.c_pro+t1.se_pro+t1.cro_pro) sum_pro
        from(
            select productId,count(*) c_pro,sum(issearch) se_pro,sum(iscrossborderproduct) cro_pro
            from etd.etd_behav_bma
            where ISNULL(productId) = 0 and length(trim(productId))>0
            group by productId
        ) t1 order by sum_pro desc limit 10""").withColumn("dt", lit(dateFormat.format(new Date().getTime)))
    dfTop10.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only", "true")
      .option("es.port", props.getProperty("es_port"))
      .option("es.nodes", props.getProperty("es_nodes"))
      .option("es.net.ssl", "true")
      .mode("append")
      .save(props.getProperty("es_path_offTopPro"))
  }

}
