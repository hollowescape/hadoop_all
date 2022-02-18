package tempi;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class reducer extends MapReduceBase implements Reducer<Text , IntWritable, Text, IntWritable>
{
	public void reduce( Text key , Iterator<IntWritable> value , OutputCollector<Text,IntWritable> values, Reporter r) throws IOException
	{
		int maxi = Integer.MIN_VALUE;
		int mini = Integer.MAX_VALUE;

		while(value.hasNext())
		{    int ans = value.next().get();
		     maxi = Math.max(maxi, ans);
		     mini = Math.min(mini, ans);
		}
		values.collect(key, new IntWritable(maxi));
		values.collect(key, new IntWritable(mini));
	}
}

