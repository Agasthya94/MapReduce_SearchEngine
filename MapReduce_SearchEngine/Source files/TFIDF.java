/* @Author: Agasthya Vidyanath Rao Peggerla.*/ 
package org.myorg;


import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;



public class TFIDF extends Configured implements Tool {

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   
	   //Counting the number of files
	   Configuration con = getConf();
	   FileSystem filesys= FileSystem.get(con);
	   Path p = new Path(args[0]);
	   final int file_count = filesys.listStatus(p).length;
	   
	   
      Job job1  = Job .getInstance(getConf(), " TFIDF-JOB1 ");
      job1.setJarByClass( this .getClass());
      
      //Adding the input and output paths
      FileInputFormat.addInputPaths(job1,  args[0]);
      FileOutputFormat.setOutputPath(job1,  new Path("TFIDF/TEMPOUTPUT"));
      
      //Setting the mapper and reducer classes
      job1.setMapperClass( FrequencyMap .class);
      job1.setReducerClass( FrequencyReduce .class);
      
      //setting the output key and value types
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( IntWritable .class);

      job1.waitForCompletion(true);
      
      
      //JOB 2
     
      con.set("FileCount",String.valueOf(file_count));
      
      Job job2  = new Job(con, " TFIDF-JOB2 ");
      job2.setJarByClass( this .getClass());
      //Adding input and output paths for job 2
      System.out.println(args[1]);
      TextInputFormat.addInputPaths(job2,  "TFIDF/TEMPOUTPUT");
      TextOutputFormat.setOutputPath(job2,  new Path(args[1]));
      
    //Setting the mapper and reducer classes
      job2.setMapperClass( IDFMap .class);
      job2.setReducerClass( IDFReduce .class);
      
      //setting the output key and value types
      job2.setOutputKeyClass( Text .class);
      job2.setOutputValueClass( Text .class);
      
      return job2.waitForCompletion( true)  ? 0 : 1;
   }
   //Code for Frequency mapper class.
   public static class FrequencyMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  //Retrieving the filename
    	  FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	  String filename = fileSplit.getPath().getName();
         String line  = lineText.toString();
         Text currentWord  = new Text();

         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            word = word.toLowerCase();
            //adding the delimiter and filename to the word 
           	word = word + "#####" + filename;  
            currentWord  = new Text(word);
            //mapper output
            context.write(currentWord,one);
         }
      }
   }
   // Code for reducer class
   public static class FrequencyReduce extends Reducer<Text ,  IntWritable ,  Text ,  NullWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         //sum of the word
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         double termfrequency = 1 + Math.log10(sum);
         //adding a tab delimiter to the word
         String outputword= word.toString() + "	" + termfrequency;
         //reducer output
         context.write(new Text(outputword),  NullWritable.get());
      }
   }
 //Code for mapper 2 class.
   public static class IDFMap extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
    	 //Retrieving the word and filename####frequency 
         String line  = lineText.toString();

         String linecontents[] = line.split("#####");
         String word = linecontents[0]; // Contains the word
         String fname_value = linecontents[1]; // Contains the filename
         
         //Retrieving the filename and trm frequency
         String contents_value[] = fname_value.split("	"); 
         String filename = contents_value[0]; //Contains the filename
         String freq_value = contents_value[1]; //Contains the term frequency
         
         String map_out = filename+"="+freq_value; //filename=frequency
         context.write(new Text(word),new Text(map_out));
         }
      }
   
   // Code for reducer 2 class
  public static class IDFReduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > files,  Context context)
         throws IOException,  InterruptedException {
    	  String filenamevalue[] = new String[100];
    	  int count = 0;
    	  Configuration conf = context.getConfiguration();
    	  String file_count = conf.get("FileCount"); //file count
    	  
    	  int num_files = Integer.parseInt(file_count);
    	  
    	  // Counting the number of files containing the word
    	  for(Text fileandvalue : files){
    		  filenamevalue[count] = fileandvalue.toString();
    		  count++;   	
    	  } 	  
    	  //Calculating IDF
    	  double idf = Math.log10(1+(num_files/count));
    	 
    	  for(int i=0;i<count;i++){
    		  String outputword = word.toString();
    		  //Retrieving the filename and word frequency
    		  String linevalues[] = filenamevalue[i].split("=");
    		  
    		  //Calculating TFIDF
    		  double tfidf = Double.parseDouble(linevalues[1]) * idf ;
    		  //word#####filename
    		  outputword = outputword+"#####"+linevalues[0];
    		  context.write(new Text(outputword),  new DoubleWritable(tfidf));
    	  }  
      }
   } 
  }





