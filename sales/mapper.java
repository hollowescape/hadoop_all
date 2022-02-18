package odd;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;


public class mapper extends MapReduceBase implements Mapper<LongWritable, Text , Text, FloatWritable>
{
	public void map(LongWritable key , Text value, OutputCollector<Text, FloatWritable> values , Reporter r) throws IOException
	{
		String[] data = value.toString().split(",");
		String payment_type = "_payment_type" + data[3];
		String country = "country_"+data[7];
		float price;
		try{
			price = Float.parseFloat(data[2]);
		
		}
		catch(Exception e)
		{
			return ;
		}

		values.collect(new Text(country), new FloatWritable(price));
		values.collect(new Text(payment_type) , new FloatWritable(1));
	}
}
