package xyz.jason5544.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class qq {
	static public void main(String[] args)
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:9000");
		conf.set("mapred.job.tracker", "master:9001");
		conf.set("mapred.jar", "C:\\Users\\Triumph\\Desktop\\qq.jar");
		try
		{
//			@SuppressWarnings("deprecation")
			Job job = new Job(conf);
			job.setJobName("qq");
			job.setJarByClass(qq.class);;
			job.setMapperClass(qqMap.class);
			job.setReducerClass(qqReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path("/qq/input/"));
			FileOutputFormat.setOutputPath(job, new Path("/qq/output/"));
			System.exit(job.waitForCompletion(true)?0:1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
