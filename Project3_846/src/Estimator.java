import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Estimator extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(Estimator.class);
	
	private Estimator() {}
	
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
			 count = context.getConfiguration().getInt("k", 2);
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
	
	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NGRAM = "n";
	private static final String COUNT = "k";
	
	 @SuppressWarnings({ "static-access" })
	    public int run(String[] args) throws Exception {
	        Options options = new Options();

	        options.addOption(OptionBuilder.withArgName("path").hasArg()
	                .withDescription("input path").create(INPUT));
	        options.addOption(OptionBuilder.withArgName("path").hasArg()
	                .withDescription("output path").create(OUTPUT));
	        options.addOption(OptionBuilder.withArgName("n").hasArg()
	                .withDescription("ngram size").create(NGRAM));
	        options.addOption(OptionBuilder.withArgName("k").hasArg()
	                .withDescription("matches required").create(COUNT));
	        

	        CommandLine cmdline;
	        CommandLineParser parser = new GnuParser();

	        try {
	            cmdline = parser.parse(options, args);
	        } catch (ParseException exp) {
	            System.err.println("Error parsing command line: "
	                    + exp.getMessage());
	            return -1;
	        }

	        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
	            System.out.println("args: " + Arrays.toString(args));
	            HelpFormatter formatter = new HelpFormatter();
	            formatter.setWidth(120);
	            formatter.printHelp(this.getClass().getName(), options);
	            ToolRunner.printGenericCommandUsage(System.out);
	            return -1;
	        }

	        String inputPath = cmdline.getOptionValue(INPUT);
	        String outputPath = cmdline.getOptionValue(OUTPUT);
	        int nGrams = cmdline.hasOption(NGRAM) ? Integer
	                .parseInt(cmdline.getOptionValue(NGRAM)) : 2;
            int matches = cmdline.hasOption(COUNT) ? Integer
	                .parseInt(cmdline.getOptionValue(COUNT)) : 2;        
	                
	        if (nGrams <= 0 || matches <= 0) {
	            System.out.println("args: " + Arrays.toString(args));
	            HelpFormatter formatter = new HelpFormatter();
	            formatter.setWidth(120);
	            formatter.printHelp(this.getClass().getName(), options);
	            ToolRunner.printGenericCommandUsage(System.out);
	            return -1;
	        }
	                
	        LOG.info("Tool name: " + Estimator.class.getSimpleName());
	        LOG.info(" - input path: " + inputPath);
	        LOG.info(" - output path: " + outputPath);
	        LOG.info(" - ngram size > 0 [default=2] : " + nGrams);
	        LOG.info(" - minimum matches > 0 [default=2]: " + matches);


	        Configuration conf= new Configuration();
			conf.setInt("n", nGrams);
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
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, tempPath);
			tempPath.getFileSystem(conf).delete(tempPath);
			job.waitForCompletion(true);
			
			//Setup second mapreduce
			Configuration conf2= new Configuration();
			conf2.setInt("k", matches);
			Job job2 = new Job(conf2,"SumDataGrams");
						
			job2.setJarByClass(Estimator.class);
			job2.setMapperClass(TokenizerMapper.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setCombinerClass(IntSumReducer.class);
			job2.setReducerClass(IntSumReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			
			Path outputPath1 = new Path(outputPath);
			FileInputFormat.addInputPath(job2, new Path("/user/mimoody/estimator/temp"));
			FileOutputFormat.setOutputPath(job2, outputPath1);
			
			outputPath1.getFileSystem(conf2).delete(outputPath1);
						
			
	        long startTime = System.currentTimeMillis();
	        
	        job2.waitForCompletion(true);
	        System.out.println("Job Finished in "
	                + (System.currentTimeMillis() - startTime) / 1000.0
	                + " seconds");

	        return 0;
	    }
	 
	public static void main(String[] args) throws Exception {
		 ToolRunner.run(new Estimator(), args);
		}
}


