package odd;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class reducer extends MapReduceBase implements Reducer<Text,IntWritable, Text , IntWritable>
{
	public void reduce(Text key, Iterator<IntWritable> value , OutputCollector<Text,IntWritable> values , Reporter r)throws IOException
	{
		int sum=0, count =0;
		if(key.equals("O"))
		{
			while(value.hasNext())
                        {
				sum += value.next().get();
				count++;
                        }

		}
		else
		{
			while(value.hasNext())
			{
				sum += value.next().get();
				count++;
			}
		}
		values.collect(key, new IntWritable(sum));
		values.collect(key, new IntWritable(count));
	}
}


