package edu.gatech.cse6242;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {
  public static class Map1 extends Mapper<Object, Text, Text, IntWritable> {
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  		String line = value.toString();
  		StringTokenizer itr = new StringTokenizer(line);
  		if (itr.hasMoreTokens()) {
        String source = itr.nextToken();
        IntWritable out = new IntWritable(1);
        String target = itr.nextToken();
        IntWritable in = new IntWritable(-1);
        context.write(new Text(source), out);
        context.write(new Text(target), in);
      }
  	}
  }

  public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
  	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
  		int sum = 0;
  		for (IntWritable val : values) {
  			sum += val.get();
  		}
  		context.write(key, new IntWritable(sum));
  	}
  }

  public static class Map2 extends Mapper<Object, Text, Text, IntWritable> {
  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  		String line = value.toString();
  		StringTokenizer itr = new StringTokenizer(line);
  		if (itr.hasMoreTokens()) {
        itr.nextToken();
        String diff = itr.nextToken();
        IntWritable one = new IntWritable(1);
        context.write(new Text(diff), one);
      }
  	}
  }

  public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
  	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
  		int sum = 0;
  		for (IntWritable val : values) {
  			sum += val.get();
  		}
  		context.write(key, new IntWritable(sum));
  	}
  }

  public static void main(String[] args) throws Exception {
    String TEMP_PATH = "./temp";
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Q4");
    job1.setJarByClass(Q4.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(TEMP_PATH));
    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "Q4");
    job2.setJarByClass(Q4.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);
    FileInputFormat.addInputPath(job2, new Path(TEMP_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
