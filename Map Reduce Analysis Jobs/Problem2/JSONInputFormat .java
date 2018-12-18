package org.apache.hadoop.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JSONInputFormat {

    public static class JSONInput extends FileInputFormat<Text, Text> {
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws
                IOException, InterruptedException {
            JSONRecordReader reader = new JSONRecordReader();
            reader.initialize(split, context);
            return reader;
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            int splitNum = 5;
            int numLinePerSplit = 15;
            Configuration conf = job.getConfiguration();
            List<InputSplit> splits = new ArrayList<>();
            for (FileStatus status : listStatus(job)) {
                Path fileName = status.getPath();
                FileSystem fs = fileName.getFileSystem(conf);
                LineReader lr = null;
                FSDataInputStream in = fs.open(fileName);
                lr = new LineReader(in, conf);
                Text line = new Text();
                int numLines = 0;
                while (lr.readLine(line) > 0) {
                    numLines++;
                }
                int numOfBlock = numLines / numLinePerSplit / splitNum + 1;
                int length = numLinePerSplit * numOfBlock;
                splits.addAll(NLineInputFormat.getSplitsForFile(status, conf, length));
            }

            return splits;
        }
    }

    public static class JSONRecordReader extends RecordReader<Text, Text> {
        private int count = 0;
        private String temp_value = "";
        private LineRecordReader lineRecordReader = null;
        private Text key;
        private Text value;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }

            Text line = lineRecordReader.getCurrentValue();
            String str = line.toString();
            if (str.contains("}") || str.contains("},")) {
                key = new Text(Integer.toString(count));
                value = new Text(temp_value.replaceAll("\"", ""));
                count++;
                temp_value = "";
                return true;
            }

            String arr = str.replaceAll("\\s+", "");
            if (!arr.equals("{")) {
                temp_value += arr;
            }
            return nextKeyValue();
        }

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }
    }

    public static class JsonMap extends Mapper<Text, Text, Text, IntWritable> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            int elevation = Integer.parseInt(line[8].split(":")[1]);
            context.write(new Text(line[5].split(":")[1]), new IntWritable(elevation));

        }
    }

    public static class JsonReduce extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException,
                InterruptedException {
            int max = 0;
            int min = 10000;
            for (IntWritable val : value) {
                if(val.get() > max){
                    max = val.get();
                }
                if(val.get() < min){
                    min = val.get();
                }
            }
            context.write(key, new Text(max + "       " + min));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JSONInputFormat");
        job.setJarByClass(JSONInputFormat.class);
        job.setMapperClass(JsonMap.class);
        job.setReducerClass(JsonReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(JSONInput.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}