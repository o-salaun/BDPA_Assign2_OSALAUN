package comparison_A;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
///////////////////////////// DRIVER CLASS

public class comparison_A extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new comparison_A(), args);	
		System.exit(res);
		   }

	@Override
	public int run(String[] args) throws Exception {
	    System.out.println(Arrays.toString(args));
	    Job job = new Job(getConf(), "comparison_A");
	    job.setJarByClass(comparison_A.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); 

	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setNumReduceTasks(1);
    
	    job.setMapOutputKeyClass(Text.class);	      
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	      	      
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
	    job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
	      
	    FileSystem fs = FileSystem.newInstance(getConf());

	    if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		// replace output file if it already exists
	        
	    job.waitForCompletion(true);			
        

	    Counters counters = job.getCounters();
	    Counter counter = counters.findCounter(COMPARISONS_COUNTER.number_of_comparisons);
	      	      

	    FileWriter writer = new FileWriter("number_of_comparisons.txt", true);
	    // save number of comparisons in a text file
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        bufferedWriter.write( String.valueOf(counter.getValue()) );	
        bufferedWriter.close();
          
	    return 0;
	}
	
	public static enum COMPARISONS_COUNTER {
		number_of_comparisons
		};
		 

//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
///////////////////////////// MAPPER CLASS

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private BufferedReader bufread;
		  
	    @Override
	    public void map(Text key, Text value, Context context)
	            throws IOException, InterruptedException {

	    	HashSet<String> id_HS = new HashSet<String>();
	    	bufread = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/comparison_A/input/part-r-00000.csv")));

	    	String pattern;
				while ((pattern = bufread.readLine()) != null) {
					String[] word = pattern.split(";");
					id_HS.add(word[0]);
					// store all keys of input file into the string hashset
				} 


			for (String id1_string : id_HS) {
			// for each element of the ID hashset...
				
				String id2_string = key.toString();
				// set id1_string as a string copy of keys

				int int1 = Integer.parseInt(id1_string);
				int int2 = Integer.parseInt(id2_string);


				if (int1 > int2 || id1_string.equals(id2_string)) {
					continue;
				// skip candidate pair when the first key is higher than the second one
				// or when the two keys are identical
				}
				
				StringBuilder pair_key = new StringBuilder();

				pair_key.append(id1_string + ";" + id2_string);
				context.write(new Text(pair_key.toString()), new Text(value.toString() ) );
				// create the keys pair
				
			}
	     }
	}
	   
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
///////////////////////////// REDUCER CLASS


	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private BufferedReader bufread;
		 
		public double simil(HashSet<String> doc1_HS, HashSet<String> doc2_HS){
			 
			HashSet<String> intersection_HS = new HashSet<String> (doc1_HS);
			// copy hashset doc1_HS into intersection_HS
			HashSet<String> union_HS = new HashSet<String> (doc1_HS);
			// copy hashset doc1_HS into union_HS

			intersection_HS.retainAll(doc2_HS);
			// keep elements of intersection_HS that also match the existing ones in doc2_HS
			union_HS.addAll(doc2_HS);
			// add elements of doc2_HS to those of union_HS without having duplicates
		     
		    int intersection_int = intersection_HS.size();
		    // compute elements in common between doc1 and doc 2
		    int union_int = union_HS.size();
		    // compute number of distinct elements in total with both doc1 and doc2
		   
		    double similarity = ((double) intersection_int) / ((double)union_int);
		    return similarity;
		    // compute Jaccard similarity
			} 

	 
	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {

	    	HashMap<String, String> copy_input_HM = new HashMap<String, String>();
	    	bufread = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/comparison_A/input/part-r-00000.csv")));	    	 
	    	String pattern;
				while ((pattern = bufread.readLine()) != null) {
					String[] word = pattern.split(";");
					copy_input_HM.put(word[0], word[1]);
					// copy input file to hashmap copy_input_HM
				}

	    	
	    	String[] double_key_string = key.toString().split(";");
	    	String key1_string = double_key_string[0];
	    	String key2_string = double_key_string[1];
	    	// separate keys from each pair from mapper output

			HashSet<String> hashset1 = new HashSet<String>();
			HashSet<String> hashset2 = new HashSet<String>();
            // create two hashsets as input for comparison in simil function

			String string1 = copy_input_HM.get(key1_string);
			String string2 = copy_input_HM.get(key2_string);
			// for each set, copy the value-document-line of copy_input_HM to a string for the corresponding key

			for (String string : string1.split(" ")) {
				hashset1.add(string);
				// transfer the content of the string to a hashset
			}
			
			for (String string : string2.split(" ")) {
				hashset2.add(string);
				// transfer the content of the string to a hashset
			}
				
			double similarity = simil(hashset1, hashset2);
            // compute similarity score between both input hashsets that correspond to the content of two documents

			context.getCounter(COMPARISONS_COUNTER.number_of_comparisons).increment(1);
            // increment the comparison counter by 1
			if (similarity >= 0.8) {
				context.write(new Text( "(" + key.toString()+ ")" ), new Text(String.valueOf(similarity)) );
				}		
	      }
	   }
}

