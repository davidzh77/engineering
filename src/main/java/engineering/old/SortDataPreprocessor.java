/**
 * 
 */
package engineering.old;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class SortDataPreprocessor {
	static class PreprocessorMapper extends MapReduceBase implements Mapper {
		private Text word = new Text();
		public void map(LongWritable  key, Text value, OutputCollector output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] tokens = line.split("\t");
			if( tokens == null || tokens.length != 2 ){
				System.err.print("Problem with input line: "+line+"n");
				return;
			}
			int nbOccurences = Integer.parseInt(tokens[1]);
			word.set(tokens[0]);
			output.collect(new IntWritable(nbOccurences),word );
		}
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void map(Object key, Object value, OutputCollector output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(SortDataPreprocessor.class);
 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
		conf.setMapperClass(PreprocessorMapper.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setNumReduceTasks(0);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		JobClient.runJob(conf);
	}
}
