import org.apache.hadoop.util.ToolRunner;
import HadoopJobDataWriter;

public class MarketingDataImporter {
	
	public static void main(final  String[] args) throws Exception{
		String[] params = new String[]{sampleData(), esIndex(), esHost() };
		int exitCode = ToolRunner.run(new HadoopDataWriter(), params);
        System.exit(exitCode);
	}
	
	public static String esIndex(){
		return "marketing/revenue";
	}
	
	public static String esHost(){
		return "<ES_HOST>:9200";
	}
	
	public static String sampleData(){
		return "sampledata.txt";
		}

}
