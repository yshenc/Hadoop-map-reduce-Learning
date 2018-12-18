package org.apache.hadoop.core;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AgeRange {

  public static class AgeRangeMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text AgeGroup = new Text();
    private Text Transtotal = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] strArray = value.toString().split("\t");
        AgeGroup.set(strArray[0]);
        Transtotal.set(strArray[1]);
        context.write(AgeGroup,Transtotal);
    }
  }
    public static class AgeRangeReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,Context context) 
        throws IOException, InterruptedException {
        float MaxTrans = 0;
        float MinTrans = 1000;
        int CountTrans = 0;
        float SumTrans = 0;
        for (Text vals: values){
            float val = Float.parseFloat(vals.toString());
            CountTrans += 1;
            SumTrans += val;
            if (val > MaxTrans){
                MaxTrans = val;
            }
            if (val < MinTrans){
                MinTrans = val;
            }
        }
            float AverTrans = SumTrans/CountTrans;
            result.set(MinTrans+","+MaxTrans+","+AverTrans);
            context.write(key, result);	
        }
    }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length != 2) {
      System.err.println("Usage: AgeRange <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "AgeRange");
    job.setJarByClass(AgeRange.class);
    job.setMapperClass(AgeRangeMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(AgeRangeReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
