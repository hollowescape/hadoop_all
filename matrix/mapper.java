package matrix;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text , Text>
{
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> values , Reporter r) throws IOException
	{
		int n=2;
		int m=3;

		String []A = value.toString().split(",");
		
		if(A[0].equals("A"))
		{
			for(int i=0;i<m;i++)
			{
				values.collect(new Text(A[1] +","+ i), new Text( "A," + A[2]+"," + A[3]));
			}
		}
		else
		{
			for(int i=0;i<n;i++)
                        {
                                values.collect(new Text(i + "," + A[2]), new Text( "B," + A[1]+"," + A[3]));
                        }

		}
	}
}
