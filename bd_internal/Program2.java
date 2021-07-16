package movie2;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Program2{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("key");

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String mstring = value.toString();// text to string
			String[] Tokens=mstring.split(",");
			if(Tokens[2].equals("Suspense"))
			output.collect(word,new IntWritable(Integer.parseInt(Tokens[3])));
			}
		}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { //{little: {1,1}} 
			int count=0;
			while(values.hasNext()) {
				count+=values.next().get();
			}
			output.collect(new Text("Total number of positive feedback from category suspense: "), new IntWritable(count));
		}
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Program2.class);
		conf.setJobName("Number of positive feedback from category suspense");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class); // hadoop jar jarname classpath inputfolder outputfolder
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
		

	}


