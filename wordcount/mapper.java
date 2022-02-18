package word;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	public void map(LongWritable key, Text value , OutputCollector<Text,IntWritable> values, Reporter r) throws IOException
	{
		String[] data = value.toString().split(" ");
		for( String s : data)
		{
			values.collect(new Text(s) , new IntWritable(1));	
		}
		
	}
}
