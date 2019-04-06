import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Estimator {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
	    
		private int nSize;
	  
		@Override
		public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException
		{
			
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
					
			String[] line = value.toString().replaceAll("[!?,.]", "").toLowerCase().split("\\s+");
			int nSize = context.getConfiguration().getInt("n", 2);
			for (int i = 0; i <= line.length - nSize; i++) {
				String ngram = "";
				for (int j = i; j < i + nSize; j++) {
					ngram += line[j];
					ngram += " ";
				}
				context.write(new Text(ngram.trim()), new Text(fileName));
			}
	            
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			List<String> vArrayList = new ArrayList<String>();
			for(Text t: values) {
				vArrayList.add(t.toString());
			}
			int count=0;
			
			NavigableMap<Pair, Integer> set = new TreeMap<Pair, Integer>();
		    for(int i = 0; i < vArrayList.size()-1; i+=2) {
		    	String file1 = vArrayList.get(i);
		    	String file2 = vArrayList.get(i+1);
		    	Pair newPair;
		    	int result = file1.compareTo(file2);
		    	if(result < 0) {
		    		newPair = new Pair(new Text(file1), new Text(file2));
		    	} else if(result == 0) {
		    		continue;
		    	} else {
		    		newPair = new Pair(new Text(file2), new Text(file1));
		    	}
		    	Pair pair = newPair;
		    	if(set!=null && set.get(pair)!=null){
					count=(int)set.get(pair);
					set.put(pair, ++count);
				}else{
					
					set.put(pair, 1);
				}
		        
		    }
		    		  
		  	for (NavigableMap.Entry<Pair, Integer> entry : set.entrySet()) { 
			  Pair k = entry.getKey(); 
			  Integer v = entry.getValue(); 
			  context.write(new Text(k.toString()), new Text(Integer.toString(v)));
			} 
			 
					
		}
	}
	
	
	 public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		 private static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		 	
		 	String line = value.toString();
		 	String[] items = line.split("\t");
		 	if(items[0].contains(",")) {
		 		one = new IntWritable(Integer.parseInt(items[1]));
		 		context.write(new Text(items[0]), one);		 		
		 	}
		 	
		 }
	 }
	 
	 public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private int count;
		 private IntWritable result = new IntWritable();
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 count = context.getConfiguration().getInt("k", 3);
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
			 }
			 if(sum >= count) {
				 result.set(sum);
				 context.write(key, result);
			 }
		 }
	 }
	
	public static void main(String[] args) throws Exception {
			
			Configuration conf= new Configuration();
			conf.set("n", args[0]);
			Job job = new Job(conf,"FindDataGrams");
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setJarByClass(Estimator.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			Path tempPath = new Path("/user/mimoody/estimator/temp");
			FileInputFormat.addInputPath(job, new Path(args[2]));
			FileOutputFormat.setOutputPath(job, tempPath);
			tempPath.getFileSystem(conf).delete(tempPath);
			job.waitForCompletion(true);
			
			//Setup second mapreduce
			Configuration conf2= new Configuration();
			conf2.set("k", args[1]);
			Job job2 = new Job(conf2,"SumDataGrams");
						
			job2.setJarByClass(Estimator.class);
			job2.setMapperClass(TokenizerMapper.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setCombinerClass(IntSumReducer.class);
			job2.setReducerClass(IntSumReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			
			Path outputPath = new Path(args[3]);
			FileInputFormat.addInputPath(job2, new Path("/user/mimoody/estimator/temp"));
			FileOutputFormat.setOutputPath(job2, outputPath);
			
			outputPath.getFileSystem(conf2).delete(outputPath);
						
			//System.exit(job.waitForCompletion(true) ? 0 : 1);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
}


