package org.apache.hadoop.core;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountryInfo{
    public static class CountryInfoMapper 
        extends Mapper<Object, Text, Text, Text> {  
            Text outkey = new Text();
            Text outvalue = new Text();
            HashMap<Integer,String> CustCountry = new HashMap<Integer,String>();
        
            protected void setup(Context context) throws IOException{
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                for (Path file: files){
                    if(file.getName().equals("Customer")){
                        BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                        String line = reader.readLine();
                        while(line != null){
                            String[] Cust = line.split(",");
                            int ID = Integer.parseInt(Cust[0]);
                            String CountryCode = Cust[4];
                            CustCountry.put(ID,CountryCode);
                            line = reader.readLine();
                        }
                        reader.close();
                    }
                }
            }
    
    public void map(Object key,Text value,Context context)
        throws IOException, InterruptedException {
        String[] record = value.toString().split(",");
        String CustID = record[1];
        String Code = CustCountry.get(Integer.parseInt(CustID));
        String TransTotal = record[2];
        outvalue.set(CustID+","+TransTotal);
        outkey.set(Code);
        context.write(outkey,outvalue);
    }
}
    
    
    public static class CountryInfoReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,Context context) 
        throws IOException, InterruptedException {
        int Count = 0;
        float MaxTrans = 0;
        float MinTrans = 1000;
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (Text vals: values){
            String[] record = vals.toString().split(",");
            int CustID = Integer.parseInt(record[0]);
            float TransTotal = Float.parseFloat(record[1]);
            if(list.contains(CustID) == false){
                Count += 1;
                list.add(CustID);
            }
            if (TransTotal > MaxTrans){
                MaxTrans = TransTotal;
            }
            if (TransTotal < MinTrans){
                MinTrans = TransTotal;
            }
        }        
            result.set(Count+","+MinTrans+","+MaxTrans);
            context.write(key, result);	
        }
    }    
        
    
    
    public static void main(String[] args) throws Exception, InterruptedException, URISyntaxException{
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length != 2) {
      System.err.println("Usage: CountryInfo <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "CountryInfo");
    job.setJarByClass(CountryInfo.class);
    job.setMapperClass(CountryInfoMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(CountryInfoReducer.class);
    DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/dataset1/Customer"), job.getConfiguration());  
    FileInputFormat.addInputPath(job, new Path(args[0]));  
    FileOutputFormat.setOutputPath(job, new Path(args[1]));  
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

