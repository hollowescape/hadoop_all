package matrix;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.util.*;
import java.io.*;

public class reducer extends MapReduceBase implements Reducer<Text,Text, Text , Text>
{
	public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> values , Reporter r) throws IOException
	{
		HashMap<Integer, Float> a = new HashMap<>();
	       	HashMap<Integer, Float> b = new HashMap<>();
		
		while(value.hasNext())
		{
			String[] A =value.next().toString().split(",");

			if(A[0].equals("A"))
			{
				a.put(Integer.parseInt(A[1]),Float.parseFloat(A[2]));
			}
			else
			{
				b.put(Integer.parseInt(A[1]),Float.parseFloat(A[2]));
			}
		}

		float res = 0.0f;

		for(int i=0;i<5;i++)
		{
			float aa = (a.containsKey(i))? a.get(i):0;
			float bb = (b.containsKey(i))? b.get(i):0;
			res += aa*bb;
		}
		values.collect(key, new Text(res + ""));

	}
}

