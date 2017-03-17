package preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AllPairWise extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		// basic check for proper input
		if (args.length != 2) {
			System.out.println("Usage: filename output_directory");
			System.exit(1);
		}
		int status = ToolRunner.run(new AllPairWise(), args); //start the job

		System.exit(status);
	}
	
	enum ComparisonNumber {TOTALCOMP}
	
	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job job = Job.getInstance(getConf(),"Allpairwise"); 
		job.setJarByClass(AllPairWise.class); 
		
		// set output key and value classes for reducer
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(DoubleWritable.class);
		
		//set output key and value classes for the mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(StudMapper.class); //set the mapper class
	
		job.setReducerClass(StudReducer.class); //set the reducer class
		job.setNumReduceTasks(1); 	//use one reducer
		
		//set input and output format classes to TextInput and TextOutput 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//get input and output directories from user of the program
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean runjob = job.waitForCompletion(true);

		//job was successful
		if (runjob) 
		{
			 FileSystem file = FileSystem.get(getConf()); 
				
			 FSDataOutputStream outFile = file.create(new Path("/home/cloudera/workspace/MDP2/comparisons.txt"));
			 Counters outcounters = job.getCounters(); 
			 Long outcompnum = outcounters.findCounter(ComparisonNumber.TOTALCOMP).getValue(); 
			  outFile.writeBytes(""+outcompnum);
			    	
			 outFile.close();	//close the stream
			return 0;
			
		} 
		//job was unsuccessful
		else 
		{
			return 1;
		}
	}


	
	public static class StudMapper extends Mapper<LongWritable, Text, Text, Text> {

//		Get  all document keys in a list of strings
		//ArrayList<Integer> DocIDs = new ArrayList<Integer>();
		
   	 private LinkedList<String> DocIDs = new LinkedList<String>();
   	 
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\\s"); 
			String curdocId = line[0]; 
			Text curDoctext = new Text(value.toString().substring(line[0].length())); 
			context.write(new Text(curdocId), new Text(curDoctext)); 
			
			for(int i = 0; i < DocIDs.size(); i++) 
			{ 
				Text KeyPair = new Text(DocIDs.get(i) + "$" + curdocId);
				context.write(KeyPair, curDoctext);
			}
			DocIDs.add(curdocId); 

		}
	}
	

	public static class StudReducer  extends Reducer<Text, Text, Text, Text> {
		
		private String[] currdoc; 
		private double threshold = .8; 
		private double js;
		 public static double JaccardSimilarity(Set<String> set1, Set<String> set2){
		
		            Set<String> intersect = new HashSet<>();
		            Set<String> union = new HashSet<>();
		            intersect.clear();
		            intersect.addAll(set1);
		            intersect.retainAll(set2);
		            union.clear();
		            union.addAll(set1);
		            union.addAll(set2);
		            return (double)intersect.size()/(double)union.size();
		        }

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String readLine = "";
			for(Text val: values) 
			{
				readLine = readLine + val.toString(); 
			}
			if(!key.toString().contains("$"))
			{ 
				currdoc = readLine.split("\\s");
			}
			else 
			{
				context.getCounter(ComparisonNumber.TOTALCOMP).increment(1);//increment the counter tracking number of comparisons
				
				String[] comp_doc = readLine.split("\\s");
				
				List<String> list1 = Arrays.asList(currdoc);
				Set<String> set1 = new HashSet<String>(list1);
				
			    List<String> list2 = Arrays.asList(comp_doc);
			    Set<String> set2 = new HashSet<String>(list2);
			    
			    js= JaccardSimilarity(set1,set2);
				
			}
			if(js>=threshold) 
			{
				context.write(key, new Text(String.valueOf(js))); 
			}
	      
	   }
	}
}

