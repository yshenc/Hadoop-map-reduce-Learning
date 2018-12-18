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

public class SpatialJoin {

  public static class SpatialMapper 
       extends Mapper<Object, Text, Text, Text>{
        Text result = new Text();
        Text out = new Text();
      
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int split = 100;
      String cf = conf.get("Input");
      int x1 = 0;
      int x2 = 10000;
      int y1 = 0;
      int y2 = 10000;
      if (cf.equals("self")){
        x1=Integer.parseInt(conf.get("x1"));
        y1=Integer.parseInt(conf.get("y1"));
        x2=Integer.parseInt(conf.get("x2"));
        y2=Integer.parseInt(conf.get("y2"));
      }
      int s_x1 = (int)(x1/split);
      int s_x2 = (int)(x2/split);
      int s_y1 = (int)(y1/split);
      int s_y2 = (int)(y2/split);

      String[] strArray = value.toString().split(",");
      if (strArray.length == 2) {
        int pointx = Integer.parseInt(strArray[0]);
        int pointy = Integer.parseInt(strArray[1]);
        int s_pointx = (int)(pointx/split);
        int s_pointy = (int)(pointy/split);
        if (pointx >= x1 && pointx <= x2 && pointy >= y1 && pointy <= y2){
          result.set(value);
          out.set(s_pointx+"|"+s_pointy);
          context.write(out,result);
        }
      } else {
        int rx1 = Integer.parseInt(strArray[1]); //blx
        int rx2 = Integer.parseInt(strArray[1])+Integer.parseInt(strArray[4]); //urx
        int ry2 = Integer.parseInt(strArray[2])+Integer.parseInt(strArray[3]); //ury
//
      	int s_rx1 = (int)(rx1/split);
      	int s_rx2 = (int)(rx2/split);
      	int s_ry2 = (int)(ry2/split); 
//
        while (s_rx1 <= s_rx2 ){
          int ry1 = Integer.parseInt(strArray[2]); //bly
          int s_ry1 = (int)(ry1/split);
          while(s_ry1 <= s_ry2 ){
            if(rx2 >= x1 && rx2 <= x2 && ry2 >= y1 && ry2 <= y2){  // upper right in the window
              out.set(s_rx1+"|"+s_ry1);
              result.set(value);
              context.write(out,result);
              }else if(rx1 >= x1 && rx1 <= x2 && ry2 >= y1 && ry2 <= y2){  // upper left in the window
              out.set(s_rx1+"|"+s_ry1);
              result.set(value);
              context.write(out,result);
              }else if(rx2 >= x2 && rx2 <= x2 && ry1 >= y1 && ry1 <= y2){  // bottom right in the window
              out.set(s_rx1+"|"+s_ry1);
              result.set(value);
              context.write(out,result);
              }else if(rx1 >= x1 && rx1 <= x2 && ry1 >= y1 && ry1 <= y2){  // bottom left in the window
              out.set(s_rx1+"|"+s_ry1);
              result.set(value);
              context.write(out,result);
              }else if(rx1 <= x1 && rx2 >= x2 && ry1 <= y1 && ry2 >= y2){  // window in the rectangle
              out.set(s_rx1+"|"+s_ry1);
              result.set(value);
              context.write(out,result);
              } 
              s_ry1 += 1;
            }
            s_rx1 += 1;
          }
        }
      }
   }


    public static class SpatialReducer extends Reducer<Text,Text,Text,Text> {
      private Text result = new Text();
      private Text output = new Text();
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            ArrayList<String> Points = new ArrayList<String>();
            ArrayList<String> Rectangles = new ArrayList<String>();
            for (Text val: value){
              String[] record = val.toString().split(",");
              if (record.length == 2) {
                  Points.add(val.toString());
              } else {
                  Rectangles.add(val.toString());
              }
            }
            for (String rect :Rectangles) {
                String[] recArray = rect.split(",");
                String Rname = recArray[0];
                int recx1 = Integer.parseInt(recArray[1]); //blx
                int recx2 = Integer.parseInt(recArray[1])+Integer.parseInt(recArray[4]); //urx
                int recy2 = Integer.parseInt(recArray[2])+Integer.parseInt(recArray[3]); //ury
                int recy1 = Integer.parseInt(recArray[2]); 
                for (String p: Points){
                  String[] poiArray = p.split(",");
                  int x = Integer.parseInt(poiArray[0]);
                  int y = Integer.parseInt(poiArray[1]);
                  if (x>=recx1 && x<=recx2 && y>=recy1 && y<=recy2) {
                    output.set("<"+Rname+"("+recx1+","+recy1+","+recx2+","+recy2+")");
                    result.set("("+p+")>");
                    context.write(output,result);
                  }
                }
              }
            }
          }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
  
    
    conf.set("mapred.textoutputformat.separator", ",");
    if (args.length == 7) {
      conf.setInt("x1", Integer.parseInt(args[3]));
      conf.setInt("y1", Integer.parseInt(args[4]));
      conf.setInt("x2", Integer.parseInt(args[5]));
      conf.setInt("y2", Integer.parseInt(args[6]));
      conf.set("Input","self");
    }else{
      conf.set("Input","null");
    }
    
    Job job = new Job(conf, "Spatial Join");
    job.setJarByClass(SpatialJoin.class);
    job.setMapperClass(SpatialMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //job.setCombinerClass(TransInfoReducer.class);
    job.setReducerClass(SpatialReducer.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
