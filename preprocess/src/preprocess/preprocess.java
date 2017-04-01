package preprocess;

import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
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

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////// DRIVER CLASS

public class preprocess extends Configured implements Tool {
  
   public static enum NUMBER_OF_LINES_COUNTER {
       TOTAL_NUMBER_OF_LINES,
        };
        
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new preprocess(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Job job = new Job(getConf(), "preprocess");
      job.setJarByClass(preprocess.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
            job.setNumReduceTasks(1); // set to 1 reduce tasks

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      FileSystem fs = FileSystem.newInstance(getConf());

      if (fs.exists(new Path(args[1]))) {
         fs.delete(new Path(args[1]), true);
      }
      
      job.waitForCompletion(true);
      
      // save number of lines in a text file
      
      long counter = job.getCounters().findCounter(NUMBER_OF_LINES_COUNTER.TOTAL_NUMBER_OF_LINES).getValue();
      Path outFile = new Path("TOTAL_NUMBER_OF_LINES.txt");
      BufferedWriter BufWri = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)));
      BufWri.write(String.valueOf(counter));
      BufWri.close();
   
          return 0;
   }
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////// MAPPER CLASS

   
  public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
   
    File stopwords_csv = new File("/home/cloudera/workspace/preprocess/StopWords.csv");
    HashSet<String> stopwords_HS = new HashSet<String>();
     
    protected void setup(Context context) throws IOException, InterruptedException {
       
    BufferedReader bufread = new BufferedReader(new FileReader(stopwords_csv));
    String string ;
    // transfer stopwords from csv file to HashSet
    while ((string = bufread.readLine()) != null){
      stopwords_HS.add(string.trim().toLowerCase());
      // need to trim string in order to remove whitespaces
      }
    bufread.close();
   }
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       
    ArrayList <String> words_AL = new ArrayList <String>();
    if (value.toString().length() != 0){
    // filter empty lines
      for (String token: value.toString().replaceAll("\\W"," ").split("\\s+")){
      // remove all special characters and split by whitespaces  
        if (!stopwords_HS.contains(token.toLowerCase())){
        // keep if token is not a stopword
          if (!words_AL.contains(token.toLowerCase())){
          // keep if word not already included in ArrayList
            words_AL.add(token.replaceAll("\\W","").toString().toLowerCase());
            word.set(token.toString());
            // word value keeps uppercase letters unlike words_AL
            context.write(key, word);
            }
            }
          }
    }
      }
   }
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

  public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
     
     
  File wordcount_csv = new File("/home/cloudera/workspace/preprocess/WordCount.csv");
   
  HashMap<String, Integer> wordcount_HM = new HashMap<String, Integer>();  
     
    protected void setup(Context context) throws IOException, InterruptedException {
      BufferedReader bufread = new BufferedReader(new FileReader(wordcount_csv));
      String string;
      
      while ((string = bufread.readLine()) != null){ 
        String[] line_content = string.split(";");
        wordcount_HM.put(line_content[0], Integer.parseInt(line_content[1]));
        // transfer the content of the wordcount csv file to the hashmap wordcount_HM
       }
       bufread.close();
      
   }
    
    public void reduce(LongWritable key, Iterable<Text> value, Context context)
              throws IOException, InterruptedException {
            
    ArrayList<String> word_string_AL = new ArrayList<String>();
    // ArrayList of words, with unsorted frequencies
    ArrayList<Integer> word_count_AL = new ArrayList<Integer>();
    // ArrayList of integer for the frequencies 
    ArrayList<String> sorted_word_AL = new ArrayList<String>();
    // ArrayList of words, with sorted frequencies


     for (Text word : value){
       word_string_AL.add(word.toString());
       // store words-values in an arraylist
       word_count_AL.add(wordcount_HM.get(word.toString()));
       // store their respective occurences in another arraylist
     }
     
          
    int i = 0;
    while(i < word_string_AL.size() + 1){
    // until we've spanned the whole ArrayList of strings
      for(int j = 0; j  < word_string_AL.size();  j++){
      // for each word of the ArrayList
        String string = word_string_AL.get(j);
        // pick the j-th word
          if(word_count_AL.get(j) == Collections.min(word_count_AL)){
          // if that j-th word has the lowest frequency...
          sorted_word_AL.add(string);
          // ...add it to the new sorted ArrayList of words...
          word_count_AL.set(j,1000000);
          // ...set the count of the j-th to 1 million to make sure it won't be selected again...
          i++;
          // increment i by 1
           
         }
       }
     }
     
     
     
      StringBuilder sorted_value = new StringBuilder();
      // store value of reduce output
       
      for (int k = 0; k < word_string_AL.size(); k++){
      // spans the whole length of the ArrayList   
         sorted_value.append(sorted_word_AL.get(k).toString());
         // append the words from sorted array list in ascending order for that document-line
         //sorted_value.append("#");
         //sorted_value.append(wordcount_HM.get(sorted_word_AL.get(k).toString().toLowerCase()));
         sorted_value.append(" ");
       }

       context.getCounter(NUMBER_OF_LINES_COUNTER.TOTAL_NUMBER_OF_LINES).increment(1);
       // increment the total number of lines by 1 for each document-line added to the reducer output
       context.write(new Text(String.valueOf(key)), new Text(sorted_value.toString()));
       
      }
   }
}