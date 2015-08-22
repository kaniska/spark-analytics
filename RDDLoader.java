



    public void createRDD(String strValue, int rowNum) {
    JavaSparkContext jsc = SparkDriver.getInstance().getSparkContext();
  	JavaRDD<String> rawDataRDD = jsc.textFile(// load file for a tenant , entity , dataset);
    
    // say productId 123 has card 9
    JavaPairRDD<Integer, Integer> cardinalityRDD = rawDataRDD.map(new PairFunction<String, Integer, Integer>() {
			private static final long serialVersionUID = -6954711200688980584L;

			@Override
			public Tuple2<Integer, Integer> call(String line) throws Exception {
	        	String[] arr = line.split(",");
		        return new Tuple2<Integer, Integer>(Integer.parseInt(strArr[0]), Double.parseDouble(strArr[1]));
	        }
	    });
    }
    
    for(Tuple2<Integer,Double> tuple : cardinalityRDD.collect()) {
			records.add(tuple);
		}
    
    // say productID 123 occurs in rowList [2,4,5,7,8,9]
    Tuple2<Integer, List<Integer>> tuple = new Tuple2<Integer, List<Integer>>(Integer.parseInt(strArr[0]), rowList);
    // write code to create ColumnarRDD from columnar data
    
    // write custom Map and Reduce filters (extends PairFunction) 
    // and apply them to rdd steps _.map and _.reduceByKey to get final aggregated result
