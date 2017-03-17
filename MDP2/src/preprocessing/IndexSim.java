package preprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.httpclient.URI;
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





public class IndexSim extends Configured implements Tool {


	public static void main(String[] args) throws Exception {
		// basic check for proper input
		if (args.length != 2) {
			System.out.println("Usage: filename output_directory");
			System.exit(1);
		}
		int status = ToolRunner.run(new IndexSim(), args); //start the job

		System.exit(status);
	}
	
	enum ComparisonNumber {TOTALCOMP}
	
	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job job = Job.getInstance(getConf(),"Indexsim"); 
		job.setJarByClass(IndexSim.class); 
		
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
			
			 FSDataOutputStream outFile = file.create(new Path("/home/cloudera/workspace/MDP2/comparisonsindex.txt"));
			 Counters outcounters = job.getCounters(); 
			 Long outcompnum = outcounters.findCounter(ComparisonNumber.TOTALCOMP).getValue(); 
			  outFile.writeBytes(""+outcompnum);
			    	
			 outFile.close();	//close the stream
			return 0;
			
		} 
		//job was unsuccessful
		else {
			return 1;
		}
	}

	
	public static class StudMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		
		
		//private ArrayList<String> doc_ids = new ArrayList<String>(); 
		private Text Docword = new Text();
		private Text currKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
			String[] line = value.toString().split("\t"); 
			currKey.set(line[0]); 
			
			LinkedList<String> dWords = new LinkedList<String>();
			
			String[] Line=line[1].split(" ");
			for(int i=0;i<Line.length;i++)
			{
				dWords.add(Line[i]);
			}
			
			//using the formula in the question: |d|-upperbound(threshold*|d|)+1
			 int upperindex = dWords.size() - (int) Math.ceil(0.8*dWords.size()) + 1;
			 
			 List<String> docselected = dWords.subList(0, upperindex);


				for (String dw: docselected) 
				{
					Docword.set(dw);
						context.write(Docword, currKey);
				}
			}
		}
	public static class StudReducer extends Reducer<Text, Text, Text, Text> 
	{
		
		private double threshold = .8; 
		private double js;
		 public static double JaccardSimilarity(Set<String> set1, Set<String> set2)
		 {
		
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


		HashMap<String, List<String>> DocKeys = new HashMap<String, List<String>>();

	
		public void setup(Context context) throws IOException, InterruptedException 
		{   Configuration conf=context.getConfiguration();
		    //conf.addResource(new Path("/hadoop/projects/hadoop-2.6.0/conf/core-site.xml"));
	        //conf.addResource(new Path("/hadoop/projects/hadoop-2.6.0/conf/hdfs-site.xml"));
			Path pt=new Path("/home/cloudera/workspace/sample.txt");
			FileSystem file = FileSystem.get(conf);
			BufferedReader Reader = new BufferedReader(new InputStreamReader(
					file.open(pt)));
			
			try { 
				String rline;

				rline = Reader.readLine();

				while (rline != null) 
				  {
				    String[] rtext = rline.split("\t");
				    List<String> contents = new ArrayList<String>();
				    String[] rLine=rtext[1].split(" ");
				    for(int i=0;i<rLine.length;i++)
					{
						contents.add(rLine[i]);
					}
				    DocKeys.put(rtext[0],contents);
					rline = Reader.readLine();
				}
				
		} 
			finally {

				Reader.close();
			}
	      
		
	}
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> LineVals = new LinkedList<String>();
			
			for (Text val: values)
			{
				LineVals.add(val.toString());
			}
			int Lsize = LineVals.size();

			if (Lsize>1){
				for (int i=0; i<Lsize-1; i++){
					for (int j=i+1; j<Lsize; j++)
                      {
						String Val1 = LineVals.get(i);
						String Val2 = LineVals.get(j);
                        
						Set<String> set1 = new HashSet<String>(DocKeys.get(Val1));
						Set<String> set2 = new HashSet<String>(DocKeys.get(Val2));
						
						js= JaccardSimilarity(set1,set2);
						context.getCounter(ComparisonNumber.TOTALCOMP).increment(1);

						if (js >= threshold)
						 {
							
							int Id1 = Integer.parseInt(Val1);
							int Id2 = Integer.parseInt(Val1);
							
			            	String KeyPair = Math.min(Id1, Id2)+ "$" + Math.max(Id1, Id2);
			                context.write(new Text(KeyPair), new Text(String.valueOf(js)));
			            }
					}
				}
			}
		}
	}
}
	
	
	
	



