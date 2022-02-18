package earth;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class reducer extends MapReduceBase implements Reducer<Text , DoubleWritable, Text, DoubleWritable>
{
	public void reduce( Text key , Iterator<DoubleWritable> value , OutputCollector<Text,DoubleWritable> values, Reporter r) throws IOException
	{
		double maxi = Double.MIN_VALUE;
		double mini = Double.MAX_VALUE;
		
		while(value.hasNext())
		{    Double ans = value.next().get();
		     maxi = Math.max(maxi, ans);
		     mini = Math.min(mini, ans);
		}
		values.collect(key, new DoubleWritable(maxi));
		values.collect(key, new DoubleWritable(mini));
	}
}

