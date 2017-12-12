package xyz.jason5544.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class qqMap extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] ss = line.split("\\s+");
		context.write(new Text(ss[0]), new Text(ss[1]));
		context.write(new Text(ss[1]), new Text(ss[0]));
	}
}
