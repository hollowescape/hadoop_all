package earth;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class mapper extends MapReduceBase implements Mapper<LongWritable, Text , Text, DoubleWritable>
{
	public void map(LongWritable key , Text value, OutputCollector<Text, DoubleWritable> values , Reporter r) throws IOException
	{
		String[] data = value.toString().split(",");
		if(data.length !=12)
		{
			return;
		}
		
		String region = data[11];
		Double magintude = Double.parseDouble(data[6]);
		

		values.collect(new Text(region), new DoubleWritable(magintude));
	}
}

