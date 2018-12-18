package org.apache.hadoop.core;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AgeSort {

  public static class AgeMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable Age = new IntWritable();
    private Text Name = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] strArray = value.toString().split(",");
      if (Integer.parseInt(strArray[2]) >= 20 && Integer.parseInt(strArray[2]) <= 50){
	Name.set(strArray[1]);
	Age.set(Integer.parseInt(strArray[2]));
	context.write(Name,Age);
      }
    }
  }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length != 2) {
      System.err.println("Usage: AgeSort <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "Age Sort");
    job.setJarByClass(AgeSort.class);
    job.setMapperClass(AgeMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
