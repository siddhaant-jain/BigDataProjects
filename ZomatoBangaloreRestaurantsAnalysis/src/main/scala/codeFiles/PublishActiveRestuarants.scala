package codeFiles

//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.functions.{col, udf}
//import org.apache.kafka.clients.admin.{AdminClient,NewTopic, CreateTopicsResult}
//import java.util.Properties
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//
//class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
//
//  lazy val producer = createProducer()
//
//  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
//}
//
//object KafkaSink {
//  def apply(config: Properties): KafkaSink = {
//    val f = () => {
//      new KafkaProducer[String, String](config)
//    }
//    new KafkaSink(f)
//  }
//}
//
object PublishActiveRestuarants extends Serializable {
//  
//  def activeRestProducer(active_restaurants: DataFrame, spark: SparkSession) = {
//    
//    val producerProp = new Properties()
//		producerProp.put("bootstrap.servers", "localhost:9092")
//		producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//		producerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//		
//		val kafkaSink = spark.sparkContext.broadcast(KafkaSink(producerProp))
//
//		active_restaurants.rdd.foreachPartition((partitions: Iterator[Row]) => {
//		  partitions.foreach((row: Row) => {
//		    kafkaSink.value.send(row.getAs("location"), row.getString(0))
//		  })
//		}) 
//    
////    CAST( struct(" + col("location").getItem(0) + ") AS STRING) AS topic
//    
////		active_restaurants.selectExpr("CAST( struct(" + col("location") + ") AS STRING) AS topic", "to_json(struct(*)) AS value")
////		  .write
////		  .format("kafka")
////		  .option("kafka.bootstrap.servers", "localhost:9092")
//////		  .option("topic", "CAST(location AS STRING)")
////		  .save()
//		
//		System.out.println("All active restaurants published")
//  }
}