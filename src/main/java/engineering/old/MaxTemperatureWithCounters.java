
package engineering.old;

import java.io.IOException;

import org.apache.hadoop.tools;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureWithCounters 
	extends Configured
	implements Tool
	{
		enum Temperature {
		MISSING,
		MALFORMED
		}
	
	static class MaxTemperatureMapperWithCounters
	extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
	
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
			int airTemperature = parser.getAirTemperature();
			output.collect(new Text(parser.getYear()),
			new IntWritable(airTemperature));
			} else if (parser.isMalformedTemperature()) {
			System.err.println("Ignoring possibly corrupt input: " + value);
			reporter.incrCounter(Temperature.MALFORMED, 1);
			} else if (parser.isMissingTemperature()) {
			reporter.incrCounter(Temperature.MISSING, 1);
			}
			reporter.incrCounter("TemperatureQuality",parser.getQuality(), 1);
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (conf == null) {
		return -1;
		}
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(MaxTemperatureMapperWithCounters.class);
		conf.setCombinerClass(MaxTemperatureReducer.class);
		conf.setReducerClass(MaxTemperatureReducer.class);
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(exitCode);
		}

}
