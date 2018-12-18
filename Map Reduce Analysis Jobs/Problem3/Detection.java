package org.apache.hadoop.core;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

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

public class Detection {

  public static class DetectionMapper 
       extends Mapper<Object, Text, Text, Text>{
      Text result = new Text();
      Text out = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      int r = Integer.parseInt(conf.get("r"));
      int split = r;
      String[] strArray = value.toString().split(",");
      int px = Integer.parseInt(strArray[0]);
      int py = Integer.parseInt(strArray[1]);
      int startx = (int)(Math.floor((px-2*r)/split)*split);
      int starty = (int)(Math.floor((py-2*r)/split)*split);
      int stopx = (int)(Math.floor((px+r)/split)*split);
      int stopy = (int)(Math.floor((py+r)/split)*split);
      for (int i = startx;i <= stopx;i = i+split){
        for (int j = starty;j <= stopy;j = j+split){
          int segx1 = i;
          int segx2 = i+split;
          int segy1 = j;
          int segy2 = j+split;
          if(segx1 >= 10000 || segx2 <= 0 || segy1 >= 10000 || segy2 <= 0){
            continue;
          }else{
            if (px >= segx1-r && px <= segx2+r && py >= segy1-r && py <= segy2+r){
              result.set(value);
              out.set(i+","+j);
              context.write(out,result);
            }
          }
        }
      }
    }
  }


    public static class DetectionReducer extends Reducer<Text,Text,Text,Text> {
      private Text result = new Text();
      private Text output = new Text();
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
          ArrayList<String> Points = new ArrayList<String>();
          ArrayList<String> inPoints = new ArrayList<String>();
          Configuration conf = context.getConfiguration();
          int r = Integer.parseInt(conf.get("r"));
          int k = Integer.parseInt(conf.get("k"));
          int split = r;
          String[] ks = key.toString().split(",");
          int wx1 = Integer.parseInt(ks[0]);
          int wx2 = Integer.parseInt(ks[0])+split;
          int wy1 = Integer.parseInt(ks[1]);
          int wy2 = Integer.parseInt(ks[1])+split;
          for (Text val: value){
            String[] record= val.toString().split(",");
            int ipx = Integer.parseInt(record[0]);
            int ipy = Integer.parseInt(record[1]);
            Points.add(val.toString());
            if (ipx > wx1 && ipx <= wx2 && ipy > wy1 && ipy <= wy2){
              inPoints.add(val.toString());
            }
          }
            //output.set(inPoints.toString());
            //result.set(Points.toString());
            //context.write(output,result);
          if (inPoints.size() != 0){
            //if(inPoints.size()-1 < k){
              for (String inp: inPoints){
                int count = 0;
                int inpx = Integer.parseInt((inp.split(","))[0]);
                int inpy = Integer.parseInt((inp.split(","))[1]);
                for (String np: Points){
                  int npx = Integer.parseInt((np.split(","))[0]);
                  int npy = Integer.parseInt((np.split(","))[1]);
                  Double dist = Math.sqrt((Math.pow((inpx-npx),2)+Math.pow((inpy-npy),2)));
                  if (dist <= r){
                    count += 1;
                  }
                }
                if (count-1 < k){
                  output.set(inpx+","+inpy);
                  result.set((count-1)+"");
                  context.write(output,result);
                }
              }
            //}
          }
        }
      }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
  
    
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length == 4) {
      conf.setInt("r", Integer.parseInt(args[2]));
      conf.setInt("k", Integer.parseInt(args[3]));
    }else{
      System.err.println("Please give r and k.");
      System.exit(2);
    }
    
    Job job = new Job(conf, "Outlier Detection");
    job.setJarByClass(Detection.class);
    job.setMapperClass(DetectionMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(2);
    //job.setCombinerClass(TransInfoReducer.class);
    job.setReducerClass(DetectionReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
