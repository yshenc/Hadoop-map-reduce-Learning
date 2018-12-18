package org.apache.hadoop.core;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Group {

  public static class GroupMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text ID = new Text();
    private Text Group = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] strArray = value.toString().split(",");
    if (strArray.length == 6){
        int Age = Integer.parseInt(strArray[2]);
        ID.set(strArray[0]);
        String group = new String();
        if (Age >= 10 && Age < 20){
            group = ("[10,20),"+strArray[3]);
        }
        else if (Age >= 20 && Age < 30){
            group = ("[20,30),"+strArray[3]);
        }
        else if (Age >= 30 && Age < 40){
            group = ("[30,40),"+strArray[3]);
        }
        else if (Age >= 40 && Age < 50){
            group = ("[40,50),"+strArray[3]);
        }
        else if (Age >= 50 && Age < 60){
            group = ("[50,60),"+strArray[3]);
        }
        else if (Age >= 60 && Age <= 70){
            group = ("[60,70],"+strArray[3]);
        }
        Group.set("1 "+group);
        context.write(ID,Group);
    }else{
	ID.set(strArray[1]);
	Group.set("2"+ " "+ strArray[2]);
	context.write(ID,Group);
	}
      }
    }

    public static class GroupReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,Context context) 
        throws IOException, InterruptedException {
        String AgeGender = new String();
        
        ArrayList<String> cache = new ArrayList<String>();
        for (Text val : values) {
          String[] record = val.toString().split(" ");
          String relationType = record[0];
          if (Integer.parseInt(relationType) == 1){
              AgeGender = record[1];
	       }else{
            cache.add(record[1]);}
        }
        for (String vals : cache) {
	    key.set(AgeGender);
            result.set(vals);
            context.write(key, result);}	
        }
      
  }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: Group <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "Group");
    job.setJarByClass(Group.class);
    job.setMapperClass(GroupMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(GroupReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
