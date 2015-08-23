import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;


public class SparkStreaming
{
  public static void main(String[] args)
  {
    SparkConf conf = new SparkConf().setAppName("SimpleNetworkLines");
    JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(3000));
    JavaReceiverInputDStream<String> streams = ctx.socketTextStream(aws_cluster1, 9999);
    
    // we can read from messaging queues like RabbitMQ , Kafka
    
    streams.print();
    ctx.start();              // Start the computation
    ctx.awaitTermination();   // Wait for the termination   
 }
}
