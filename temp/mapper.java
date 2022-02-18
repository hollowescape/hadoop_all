package tempi;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class mapper extends MapReduceBase implements Mapper<LongWritable, Text , Text, IntWritable>
{
	public void map(LongWritable key , Text value, OutputCollector<Text, IntWritable> values , Reporter r) throws IOException
	{
		String s = value.toString();
		String s1 = s.substring(15,19);

		int temp;

		if(s.charAt(87) =='+')
		{
			temp = Integer.parseInt(s.substring(88,92));
		}
		else
		{
			temp = Integer.parseInt(s.substring(87,92));
		}
		values.collect( new Text(s1), new IntWritable(temp));
	}
}

