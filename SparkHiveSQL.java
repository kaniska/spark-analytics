

public class SparkHiveSQL
{
  public static void main(String[] args)
  {
    SparkConf sparkConf = new SparkConf().setAppName("SparkSQLSortingCaching");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    HiveContext hiveContext = new HiveContext(jsc.sc());
    hiveContext.setConf( "spark.sql.inMemoryColumnarStorage.compressed", "true" );
    
    DataFrame consumers = hiveContext.sql("SELECT consumer_name FROM consumers");
    // Map consumers to <consumer, 1>
    JavaPairRDD<String, Integer> consumerIntermediateCounts = consumers.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>()
    {
      @Override
      public Tuple2<String, Integer> call(Row r) throws Exception
      {
        return new Tuple2<String, Integer>( r.getString(0), 1 );
      }
    });  
  
    JavaPairRDD<String, Integer> consumerAggCount = consumerIntermediateCounts.reduceByKey(new Function2<Integer, Integer, Integer>()
    {
      @Override
      public Integer call(Integer c1, Integer c2) throws Exception
      {
        return c1 + c2;
      }
    });  
    
  JavaPairRDD<String, Integer> consumerSorted = consumerAggCount.sortByKey(true);

  List<Tuple2<String, Integer>> consumerFinalOut = consumerSorted.collect();  
    
  }
}
