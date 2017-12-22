/* @Author: Agasthya Vidyanath Rao Peggerla. */ 

package org.myorg;


import java.io.IOException;
import java.util.regex.Pattern;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class DocWordCount extends Configured implements Tool {

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   //Creating a new job
      Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());
      
      //Adding the input and output paths
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      //Setting the mapper and reducer classes
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      
      //setting the output key and 
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   //Code for mapper class.
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  //Retrieving the filename
    	  FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	  String filename = fileSplit.getPath().getName();
         String line  = lineText.toString();
         String line1 = line.trim();
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
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  NullWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         //sum of the word
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         //adding a tab delimiter to the word
         String outputword= word.toString() + "	" + sum;
         //reducer output
         context.write(new Text(outputword),  NullWritable.get());
      }
   }
}
