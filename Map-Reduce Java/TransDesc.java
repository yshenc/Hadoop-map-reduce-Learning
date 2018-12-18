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

public class TransDesc {

  public static class TransDescMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text ID = new Text();
    private Text Need = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] strArray = value.toString().split(",");
    if (strArray.length == 6){
      	ID.set(strArray[0]);
	Need.set("1"+ ","+ strArray[1]+","+strArray[5]);
	context.write(ID,Need);
    }else{
	ID.set(strArray[1]);
	Need.set("2"+ ","+ strArray[2]+","+strArray[3]);
	context.write(ID,Need);
	}
      }
    }

   

   public static class TransDescReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private String inner = new String();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      float sumT = 0;
      int countT = 0;
      int MinItem = 11;
      for (Text val : values) {
	String[] record = val.toString().split(",");
        String relationType = record[0];
	if (Integer.parseInt(relationType) == 2){
	   sumT += Float.parseFloat(record[1]);
	   countT += 1;
	   if (Integer.parseInt(record[2]) < MinItem){
	      MinItem = Integer.parseInt(record[2]);
	   }
	}
	if (Integer.parseInt(relationType) == 1) {
	   inner = record[1]+","+record[2]; 	
	}
}
      result.set(inner + "," + countT +"," + sumT +","+MinItem);
      context.write(key, result);
	
    }
  }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length != 2) {
      System.err.println("Usage: TransDesc <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "Transaction Desc");
    job.setJarByClass(TransDesc.class);
    job.setMapperClass(TransDescMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //job.setCombinerClass(TransInfoReducer.class);
    job.setReducerClass(TransDescReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
