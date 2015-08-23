import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import MarketingData;

public class HadoopDataWriter extends Configured implements Tool {

	 Logger logger = LoggerFactory.getLogger(HadoopDataWriter.class);
	 
		public static class MarketingDataImportMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		    Logger logger = LoggerFactory.getLogger(MarketingDataImportMapper.class);
		    private ObjectMapper jsonMapper;
		    private SimpleDateFormat dateFormatter =
		    		 new SimpleDateFormat("dd-MM-yyyy");
		    private Text buildDataText;
	 
			@Override
			protected void map(LongWritable key, Text value,
					Mapper<LongWritable, Text, NullWritable, Text>.Context context)
					throws IOException, InterruptedException {
				logger.debug("Entering MarketingDataImportMapper.map()");
				try {
					String jobData = value.toString();
					System.out.println("Value of Job Data " + jobData);
					String[] jobDataList= jobData.split(",");
					
					System.out.println("JobDataList "  + Arrays.toString(jobDataList) +" length -> " + jobDataList.length);
					
					if(jobDataList.length > 0) {
						String jobName = jobDataList[0];
						String jobStatus = jobDataList[1];
						Date jobRunTime = dateFormatter.parse(jobDataList[2]);
						String jobType = jobDataList[3];
						
						MarketingData marketingData = new MarketingData(jobName, jobStatus,
								jobType, jobRunTime);
	 
						String buildDataJSON = jsonMapper.writeValueAsString(buildData);
						buildDataText.set(buildDataJSON);
						
						//System.out.println("Data to be inserted into ES : "+ buildDataText);
	 
						context.write(NullWritable.get(), buildDataText);
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				logger.debug("Exiting MarketingDataImportMapper.map()");
			}
	 
			@Override
			protected void setup(
					Mapper<LongWritable, Text, NullWritable, Text>.Context context)
					throws IOException, InterruptedException {
				jsonMapper = new ObjectMapper();
				buildDataText = new Text();
			}
			
		}
		
		public int run(String[] args) throws Exception {
			if (args.length != 3) {
	            System.err.printf("Usage: %s [generic options] <input> <output> <ES host>\n",
	                    getClass().getSimpleName());
	            ToolRunner.printGenericCommandUsage(System.err);
	            return -1;
	        }
	 
	         logger.info("Input path " + args[0]);
	        logger.info("Output path " + args[1]);
	 
	        Configuration conf = new Configuration();
	        conf.setBoolean("mapred.map.tasks.speculative.execution", false);    
	        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false); 
	        conf.set("es.input.json", "yes");
	        conf.set("es.nodes", args[2]);
	        conf.set("es.resource", args[1]);                            
	        Job job = new Job(conf);
	        job.setJarByClass(HadoopDataWriter.class);
	        job.setJobName("MarketingDataImporter");
	        
	        job.setOutputFormatClass(EsOutputFormat.class);
	        job.setMapOutputValueClass(MapWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	 	   
	        job.setNumReduceTasks(0);
	        
	        job.setMapOutputKeyClass(NullWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        
	        job.setMapperClass(JobDataImportMapper.class);
	 
	        int returnValue = job.waitForCompletion(true) ? 0:1;
	        String status = job.isSuccessful() ? "Job is Successful !" : "Job has Failed !";
	        System.out.println(status);
	        return returnValue;
		}
	 
	}
