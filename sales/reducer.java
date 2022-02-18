package odd;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class reducer extends MapReduceBase implements Reducer<Text , FloatWritable, Text, FloatWritable>
{
	public void reduce( Text key , Iterator<FloatWritable> value , OutputCollector<Text,FloatWritable> values, Reporter r) throws IOException
	{
		String temp = key.toString();
		if(temp.substring(0,8)=="country_")
		{	float total_sales =0;
			while(value.hasNext())
			{
				total_sales += value.next().get();
			}
			values.collect(key, new FloatWritable(total_sales)); 
		}
		else
		{
			float payment_type =0;
                        while(value.hasNext())
                        {
                                payment_type += value.next().get();
                        }
                        values.collect(key, new FloatWritable(payment_type));

		}
	}
}

