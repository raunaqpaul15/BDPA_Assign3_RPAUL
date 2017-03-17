package preprocessing;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PreDoc extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Usage: filename output_directory");
			System.exit(1);
		}
		int status = ToolRunner.run(new PreDoc(), args); 
		System.exit(status);
	}
	
	@Override
	public int run(String args[]) throws Exception {
		
		Job job = Job.getInstance(getConf(),"Preprocessing"); 
		job.setJarByClass(PreDoc.class); 
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		

		job.setMapperClass(StudMapper.class); 
		job.setNumReduceTasks(1); 
		
		job.setReducerClass(StudReducer.class); 
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean runjob = job.waitForCompletion(true);

		//job was successful
		if (runjob) {
			
			
			 FileSystem fs = FileSystem.get(getConf()); 
		
			 FSDataOutputStream outputf = fs.create(new Path("/home/cloudera/workspace/MDP2/Preprocessed Records.txt"));
			 Counters outputcounters = job.getCounters(); 
			 Long outputrecordnumber = outputcounters.findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue(); 
			  outputf.writeBytes(""+outputrecordnumber);
			    	
			 outputf.close();	
			return 0;
		} 
		
		else {
			return 1;
		}
	}
 
	public static class StudMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private HashSet<String> stopwords; 
		private Text pword = new Text();
		private String did_hash = "########################";
		private final static Text ONE = new Text("1"); 
	
	     @Override
	     protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
	    	
	    	 FileSystem file = FileSystem.get(context.getConfiguration());
				stopwords = new HashSet<String>(); 
				
	    	 
	    	 
	    	 try {

					BufferedReader Reader = new BufferedReader(new InputStreamReader(
							file.open(new Path("/home/cloudera/workspace/MDP2/stopwords.txt")))); // open the file
					try { 
						String line;

						line = Reader.readLine();

						while (line != null) { //read through the file, getting all the stop words
							String[] s_word = line.split(" ");
							stopwords.add(s_word[0]);

							line = Reader.readLine();
						}
					} finally {

						Reader.close();
					}
				} catch (IOException e) {
					System.out.println(e.toString());
				}
	    	 
				String[] Line = value.toString().toLowerCase().split("\\W"); 
				for (int i = 0; i < Line.length; i++) {
					if (!stopwords.contains(Line[i]) && !Line[i].isEmpty()) { 
						String key1 = "" + key; 
						String nkey = did_hash.substring(0,did_hash.length() - key1.length()) + key1; 
						pword.set(Line[i]);
						context.write(pword, ONE);
						context.write(new Text("{" + nkey), pword); 
					}
				}

			}
		}
	

	public static class StudReducer extends Reducer<Text, Text, LongWritable, Text> {
		
		private HashMap<String, Integer> frequencies = new HashMap<String, Integer>(); 
		private long counter = 0; 
		
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int wordfreq = 0; 
			LinkedList<String> LineWords = new LinkedList<String>(); 
			for(Text val : values) {
				if(key.charAt(0) != '{') { 
					wordfreq += Integer.parseInt(val.toString());
				} 
				else { 
					String[] wvalue = val.toString().split("\\s");
					for(int i = 0; i < wvalue.length; i++) {
						int index = 0;
						String curword = wvalue[i];
						if(!LineWords.contains(curword)) { 
							int currfreq = frequencies.get(curword);
							while(index < LineWords.size() && currfreq > frequencies.get(LineWords.get(index))) { 
								index++;
							}
							LineWords.add(index, curword); 
						}
					}
				}
			}
			if(key.charAt(0) != '{') { 
				frequencies.put(key.toString(), wordfreq);
			}
			else {
				String ckey = key.toString();
				int startind = ckey.lastIndexOf("#") + 1;
				long docid = Long.parseLong(ckey.substring(startind));
				String finaloutput ="";
				for(String s : LineWords) {
					finaloutput = finaloutput + s + " "; 
				}
				if(counter == 0) { 
					counter = docid + 1;
				}
				context.write(new LongWritable(counter), new Text(finaloutput)); 
				counter++;
			}
		}
	}
}
