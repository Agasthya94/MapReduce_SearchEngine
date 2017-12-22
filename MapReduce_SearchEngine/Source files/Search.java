/* @Author: Agasthya Vidyanath Rao Peggerla. */ 
package org.myorg;


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class Search extends Configured implements Tool {

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   
	   //Creating a new job
	   
	  Configuration con = getConf();
	  con.set("Query",args[2]);
      Job job  =new Job(con, " Search ");
      job.setJarByClass( this .getClass());
      
      //Adding the input and output paths
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      //Setting the mapper and reducer classes
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
     
      //setting the output key and 
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   //Code for mapper class.
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
    	 Configuration config = context.getConfiguration();
    	 String userQuery = config.get("Query");
    	 String userQuery1 = userQuery.toLowerCase();
    	 // Splitting the query and storing the words into a string array
    	 String s[] = userQuery1.split(" ");
         String line  = lineText.toString();
         
         //searching all the words in the query
         for ( String userword  : s) {
        	 //checking if the word in the query is present in the file
            if (line.contains(userword)) {
               String word[] = line.split("#####");
               //retrieving the words from the file
               String remainingword = word[1];
               String fileandtfidf[] = remainingword.split("	");
               //Splitting the filename and tfidf
               String filename = fileandtfidf[0];
               String tfidf = fileandtfidf[1];
               double t = Double.parseDouble(tfidf);
               
               // <filename tfidf>
               context.write(new Text(filename),new DoubleWritable(t));
            }
 
         }
      }
   }
   // Code for reducer class
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0;
         //sum of the tfidf values
         for ( DoubleWritable count  : counts) {
            sum  += count.get();
         }
        
         String outputword= word.toString();
         //reducer output -- filename tfidf
         context.write(new Text(outputword),new DoubleWritable(sum));
      }
   }
}
