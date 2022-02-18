package odd;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text , IntWritable>
{
	public void map(LongWritable key , Text value , OutputCollector<Text, IntWritable> values, Reporter r) throws IOException
	{
		String []data = value.toString().split(" ");
		for(String s : data)
		{
			if(Integer.parseInt(s)%2!=0)
			{
				values.collect(new Text("O"),new IntWritable(Integer.parseInt(s)));
			}
			else
			{
				values.collect(new Text("E"),new IntWritable(Integer.parseInt(s)));
			}
		}

	}


}
