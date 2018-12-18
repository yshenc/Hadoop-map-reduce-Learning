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



public class TransInfo {
	
	/*private static class IntArrayWritable extends ArrayWritable{

		public IntArrayWritable() {
			super(IntWritable.class);
		}
		
		public IntArrayWritable(String[] strings){
			super(IntWritable.class);
			IntWritable[] nums = new IntWritable[strings.length];
			for (int i = 0; i < strings.length; i ++){
				nums[i] = new IntWritable(Integer.parseInt(strings[i]));
			}
			set(nums);
		}
		
	}*/
	
	public static class TransInfoMapper extends  Mapper<Object, Text, Text, Text>{
		private Text ID = new Text();
        private Text Trans = new Text();
		HashMap<Integer,String> CustInfo = new HashMap<Integer,String>();
        
        protected void setup(Context context) throws IOException{
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path file: files){
                if(file.getName().equals("Customer")){
                    BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                    String line = reader.readLine();
                    while(line != null){
                        String[] Cust = line.split(",");
                        int ID = Integer.parseInt(Cust[0]);
                        String Name = Cust[1];
                        String Salary = Cust[5];
                        CustInfo.put(ID,Name);
                        line = reader.readLine();
                    }
                    reader.close();
                }
            }
        }
        
		public void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
			String[] record = value.toString().split(",");
			String CustID = record[1];
            String Name = CustInfo.get(Integer.parseInt(CustID));
			String[] vals = new String[2];
            ID.set(CustID+","+Name);
            Trans.set("1"+","+record[2]);
			context.write(ID,Trans);
		}
		
	}
	
	private static class TransInfoReducer extends Reducer<Text, Text, Text, Text>{
        private Text output =new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			float totalSum = 0;
			for (Text val : values){
                String[] record = val.toString().split(",");
				count += Integer.parseInt(record[0]);
				totalSum += Float.parseFloat(record[1]);
			}
			output.set(count+","+totalSum);
			context.write(key,output);
		}
	}

	public static void main(String[] args) throws Exception, InterruptedException, URISyntaxException{
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length != 2) {
      System.err.println("Usage: TransInfo <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "TranInfo");
    job.setJarByClass(TransInfo.class);
    job.setMapperClass(TransInfoMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setCombinerClass(TransInfoReducer.class);
    job.setReducerClass(TransInfoReducer.class);
    DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/dataset1/Customer"), job.getConfiguration());  
    FileInputFormat.addInputPath(job, new Path(args[0]));  
    FileOutputFormat.setOutputPath(job, new Path(args[1]));  
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}