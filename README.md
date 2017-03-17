# BDPA_Assign3_RPAUL
Assignment 3 of Hadoop Under Massive Data Processing Course At Centrale Supelec, Spring 2017


#MASSIVE DATA PROCESSING ASSIGNMENT III

## 1. Pre-processing the input

In this assignment, you will use the document corpus of pg100.txt, as in your previous assignments,
assuming that each line represents a distinct document (treat the line number as a document
id). Implement a pre-processing job in which you will:
* Remove all stopwords (you can use the stopwords file of your previous
assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines.
* Store on HDFS the number of output records (i.e., total lines).
* Order the tokens of each line in ascending order of global frequency.
Output the remaining lines. Store them on HDFS in TextOutputFormat in any order.


### SOLUTION

For this above problem, we will be using the stopwords.txt file created in the first assignment with the document corpus of pg100.txt containing the entire work of William Shakespeare. The stopwords file was generated in the previous assignment and contains words with a frequency > 4000 and will be removed during the preprocessing. 

#### Reading the Stopwords File:

First, we need to read the stopwords from stopwords.txt. Once we do that we will have the list of words which we have to filter out in order to generate the Preprocessed document. The preprocessed document will contain the line number and the words in pg100.txt which do not match with the ones in stopwords.


```
		stopwords = new HashSet<String>(); 
				 try {
					BufferedReader Reader = new BufferedReader(new InputStreamReader(
							file.open(new Path("/home/cloudera/workspace/MDP2/stopwords.txt"))));
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
					
```
We store the stopwords in a hashset and use a bufferedreader to read the words line by line, split them and add them to the stopwords set. 

#### The Mapper Method:

The mapper function first takes inputs from the pg100.txt file and then compares line by line with the stopwords set and only outputs those words which dont belong to the stopwords set with the corresponding line whose number is treated as the document/line id. 

```
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

```

The mapper writes the current key and the non stopword words in that line to the context and thus the Reducer receives in the (LongWritabole key,Text words) format. 

#### The Reducer Method:

The reducer method not only has to generate the preprocessed document but each line should have the words sorted in ascending order according to their frequencies. It also needs to count the number of documents being created in the preprocessed document. 

Sorting into ascending order of frequencies: 

We create a linkedlist LineWords to store the words and wordfreq(integer type) to store the frequency of the words. We use a string array to split the words from each line and then iterate through the array using a for loop. We use curword to store the current word in the string whose frequency we will compare with the rest of the words in the string and change the index to sort it. Once the sorting is done , we add the pair of (index,current word) to the linkedlist. 

```
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
```

#### RECORDS COUNTER 

We run the following code in the main class to obtain the number of records we get in the output text file considering the job ran successfully. Hadoop has an inbuilt counter which we invoke to find the number of records which the Reducer generated. 

```
if (runjob) {
			
			
			 FileSystem fs = FileSystem.get(getConf()); 
		
			 FSDataOutputStream outputf = fs.create(new Path("Preprocessed Records.txt"));
			 Counters outputcounters = job.getCounters(); 
			 Long outputrecordnumber = outputcounters.findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue(); 
			  outputf.writeBytes(""+outputrecordnumber);
			    	
			 outputf.close();	
			return 0;
		} 
		
```
#### OUTPUT
The text file generated is in the following format:
```
1	ebook complete shakespeare works gutenberg project william by of the 
2	shakespeare william 
3	anyone anywhere ebook cost use at no this with for is of and the 
4	restrictions whatsoever copy almost away give may or no it you 
5	included license terms under re gutenberg project use it of the 
6	online www ebook org gutenberg at or this with 
```
![Alt text](https://www.dropbox.com/home?preview=allpairwise.JPG "preproc")

## 2. SET SIMILARITY JOIN ( Using ALL PAIR WISE Comparisons) 

Perform  all  pair wise  comparisons  between  documents,  using  the  following technique:

* Each  document  is  handled  by  a  single  mapper  (remember  that  lines  are used to represent documents in this assignment).
* The map method should emit, for each document, the document id along with one other document id as a key (one such pair for each other document in the corpus) and the document’s content as a value.
* In the reduce  phase,  perform the Jaccard computations for all/some selected pairs. 
* Output only similar pairs on HDFS, in TextOutputFormat.
* Make sure that the same pair of documents is compared no more than once.
* Report the execution time and the number of performed comparisons.


### SOLUTION

As required by the problem, the output file generated from the preprocessing process will be used as input in this case. The preprocessed text file has a unique value for each line as line number which will be considering as document ID  and it's filtered sorted corpus.

Now, I haven't used a seperate filereader to read the sample file as I have stored in the input folder and used that as argument for program.

#### THE MAPPER METHOD 


The mapper method follows the following technique:

* Create a list of string format (DocIDs) to store all the document IDs or keys and then the Mapper traverses the entire sample file. 
*  I used a string array to split and store the words from each line as the mapper reads one line at a time , thus satisfying our requirement of using a mapper for each line. Now the splitting is done for all alphanumeric words and thus at the first index i.e. index=0 of the line array, the ID in numeric format is obtained. For splitting and obtaining only the alphanumeric words , we use the regex ('\\\s') as mentioned under java documentation. 
*  I now used another string ( curDocID) to store just the Key from the line array and a seperate Text format variable (curDoctext) to store the rest of the line which the text values ( words in that line ). This way we have seperate variables storing the ID Key and Text Values from each document ( line ) and then we write the pair of (ID Key,Text Values) to the context for each document.
*  Now  loop through the size of string list DocIDs and use the current document id to iterate in order to create the keypair and store the values of the second document ( current document) in the key pair(KeyPair) .
So, for example if the loop is reading the current id=4 that is reading the line 4 and the size of the List containing IDs is 4, we are creating the following pairs:
 (<1,4>;value of line 4) ,(<2,4>;value of line 4),(<3,4>;value of line 4) and so on.

*  Finally, write  pair of (KeyPairs, Value of current document) to the context and we add the current document id to the DocIDs list to ensure that we have mapped that line already.

The Mapper method needs to emit (id1$id2, value of id2). This way we are sure that the mapper is emiting the desired output for the reducer to use. 

```
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
```

#### THE REDUCER METHOD 

The main function of the reducer method is to compare similarities between two lines based on the Jaccard Similarity and generate all those lines pairs which have a similarity >= 0.8.
The following is an example of Jaccard Similarity:

d1: “I  have a  dog”

d2: I have a cat”

d3: “I have a big dog”

sim(d1,d2)=3/5 =0.6 < 0.8  : d1       and       d2       are not similar

sim(d2,d3)=3/6=0.5  < 0.8 : d2       and       d3       are not similar

sim(d1,d3) = 4/5 = 0.8 :d1 and d3 are similar

The following code implements the Jaccard Similarity function and will be used for the entire assignment. 

```
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

```
The clear function removes all elements and makes sure that your set is empty
The addAll function adds all the elements in the specified collection to this set if they are not already present.If the specified collection is also a set, the operation will modify this set so that its value is the union of the two sets.
The retainAll function retains only the elements in this set that are contained in the specified collection i.e. it removes all those elements which aren't in the specified collection. It effectively does an intersection of two sets.


The Reducer follows the following technique:

* We first set the number of reducers to 1 as required by our problem in the main function.
```
job.setNumReduceTasks(1); 
```
* Then, we create a string array (currdoc) to store the current doc values and double variables: js to calculate the jaccard similarity and threshold. We initialize the threshold as 0.8
*  Now in the Reduce function, we create a string ( readLine ) and we iterate to add the values derived from the mapper. 
*   Now the key in the Reduce function is the keypair generated by the mapper.
*   If there is no "$" seperator, we can just store the values to the currdoc else we run the comparison part.
*   In the comparison portion, we read the two different document values, one belonging to the current doc with the other . We convert both strings to hash set to pass as arguments to the Jaccard Similarity function in order to get the Similarity score for that particular keypair combination. In the comparison portion we also add the following counter to keep a count on the number of comparisons. 
```
context.getCounter(ComparisonNumber.TOTALCOMP).increment(1);
```
*   Finally, we check for those keypairs which have a Jaccard similarity greater than or equal to the threshold and write it to the file. 

```
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
```

#### THE COMPARISONS NUMBER COUNTER

We need to count the number of comparisons made by our Reducer on our input sample text file and then write it to a file in the file system. For this , we create a global enumerator in the following way:
```
enum ComparisonNumber {TOTALCOMP}
```
And in the main function, if the job ran succesfully , we add the following code to write to the file system.
```
boolean runjob = job.waitForCompletion(true);

		//job was successful
		if (runjob) 
		{
			 FileSystem file = FileSystem.get(getConf()); 
				
			 FSDataOutputStream outFile = file.create(new Path("comparisons.txt"));
			 Counters outcounters = job.getCounters(); 
			 Long outcompnum = outcounters.findCounter(ComparisonNumber.TOTALCOMP).getValue(); 
			  outFile.writeBytes(""+outcompnum);
			    	
			 outFile.close();	//close the stream
			return 0;
			
		} 
```
The Counters object stores all the counters generated by the job. We use the findCounter() function to find the counter we defined to count the comparisons. 

#### OUTPUT 

There are two files generated as outputs. One contains the pair of lines and similarity score, the other just contains the number of comparisons generated. As input , I have used the first 500 lines as sample input from the preprocessed document as running the entire the preprocessed document with over 100k lines would take much longer computational time. 

Comparisons File output has the following :
```
Number of Comparisons= 124750
```
The Main output file has the following values:
```
2#133	1.0
23#123	1.0
23#39	1.0
24#124	1.0
24#40	0.8
25#125	0.8
26#126	1.0
27#127	1.0
28#128	1.0
29#129	1.0
30#130	1.0
39#123	1.0
40#124	0.8
```


![Alt text](https://www.dropbox.com/home?preview=preprocessing.JPG "preproc")



## 2. SET SIMILARITY JOIN ( Using INVERTED INDEX Comparisons) 

* Create an inverted index, only for the first $|d| - [t |d|] + 1$ words of each document d (remember that they are stored in ascending order of frequency). 
* In your reducer, compute the similarity of the document pairs. 
* Output only similar pairs on HDFS, in TextOutputFormat. 
* Report the execution time and the number of performed comparisons.



### SOLUTION

As required by the problem, the output file generated from the preprocessing process will be used again as input in this case. The preprocessed text file has a unique value for each line as line number which will be considering as document ID  and it's filtered sorted corpus.Now, in this case we will be taking a certain subset of each line and we will try to perform the similarity match. The subset of each line will be based on the first  $|d| - [t |d|] + 1$ words in each line. 
 
The approach is to reduce the number of intemediate comparisons by indexing the words first.The word is indexed with all the document ids it is present in.This searches for  similar pairs only among the indexed ids of a given word. This approach was stated under Filtered SSJoin implementation in the paper published under Microsoft Research ["A Primitive Operator For Similarity Joins in Data Cleaning", by Chaudhuri,Ganti,Kaushik; 2006] at ICDE,2006. 

e.g. Consider the sets s1={1,2,3,4,5} and s2={1,2,3,4,6}. They have an overlap of 4 and any subset of either set will have a nonzero overlap with the other. Thus instead of perfroming a normal equijoin, we can perform a equijoin with a smaller filtered subset. Thus the resultant intermediate joins are reduced by a significant margin. 


The mapper uses the following method:

* We start our approach similar to the All Pair Wise Comparison in the previous problem. We first create two variables  Docword & currKey to store the textvalues and the current key/id of the line( document )
*  We create a string array , line to split the numeric id and text portion of each line splitting at the tab using the regex ("\t") and then store the id in currKey
*   We then created a linkedlist of strings ( dWords ) and a string ( Line ). The string Line will just store the textual portion of each line which we have obtained by splitting earlier. We add those words into a list format in dWords after that. 
*  We now calculate an upperindex based on the formula  $|d| - [t |d|] + 1$  in order to get our substring for comparisons. Math.ceil function was used since the formula has it , instead of Math.round. The value of d= size of dwords and t=threshold=0.8 in this case .
```
int upperindex = dWords.size() - (int) Math.ceil(0.8*dWords.size()) + 1;
```
* We created another string list , docselected to store the documents with only words belonging to the substring based on the upperindex calculated in the previous step. 
```
List<String> docselected = dWords.subList(0, upperindex);
```
* Looping through docselected using a for loop, store the strings in Docwords and then write to context along with the key in the format (Docwords,currkey)
```
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
```
#### THE REDUCER METHOD

The reducer method's main function is same as in the previous problem i.e. compare the documents and their jaccard similarities , then write to the file the two document ids and their similarity if the similarity is greater than equal to the threshold=0.8.
In the previous problem, I had used linked lists alone but in this, I have used a HashMap of the list of keys. 
The Reducer function follows the following steps:

* Create a HashMap, Dockeys mapping from string to a string list.  
* Use a setup context function to read from the input file using buffered file reader. Parse the lines into id and text parts and store them in Dockeys. 
```
public void setup(Context context) throws IOException, InterruptedException 
		{
			
			FileSystem file = FileSystem.get(context.getConfiguration());
			BufferedReader Reader = new BufferedReader(new InputStreamReader(
					file.open(new Path("/home/cloudera/workspace/MDP2/input1/sample.txt"))));
			
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

```
* The reducer gets iterable <text> values from the mapper.All the index ids containing a particular word are stored in the list LineVals for that corresponding word. 
```
List<String> LineVals = new LinkedList<String>();
			
			for (Text val: values)
			{
				LineVals.add(val.toString());
			}
```
* Now I ran a nested for loop in the following way with Lsize being the size of LineVals in order to create the pairs :
```
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

```
This creates the pairs, and obtains values from the hashmap with the jaccard compared similarities. The pairs satisfying the corresponding threshold are then taken and appended together before writing to the context file. 

The comparisons count is same as the one we used in the previous problem and we follow the same process to write to a file. 

#### OUTPUT
Sample input is same as the previous problem i.e. first 500 lines of the preprocessed output text file. 

The number of comparisons
```
168
```
Output file with Similarities
```
27:127	1.0
24:124	1.0
24:40	1.0
40:124	1.0
25:125	1.0
29:129	1.0
39:123	1.0
23:39	1.0
23:123	1.0
1:15	0.875
25:125	1.0
26:126	1.0
30:130	1.0
28:128	1.0
38:121	0.8
2:133	1.0
39:123	1.0
```


## 3. COMPARING RESULTS 

| METHOD     | Number of Comparisons| Execution Time |
|:-----------|------------:|:------------:|
| PairWise       |       124750 |     21 secs     |
| Inverted Index    |      168 |    15 secs     |

We see that even with a small sample the difference in execution times is  significant.
We can also see the the number of comparisons in case of Inverted Index is around 1/740th of that AllPairWise. This is because of the reduced intermediate join pairs created in case of inverted index. Not only that , the jaccard similarity is calculated for way less number of time, and thus the cost of calculating addall, retainall functions is significantly less for Inverted Index method. 

### REFERENCES
S. Chaudhuri, V. Ganti, and R. Kaushik. A primitive operator for similarity joins in data cleaning. In Proceedings of the 22nd International Conference on Data Engineering, ICDE 2006, 3-8 April 2006, Atlanta, GA, USA,2006.
