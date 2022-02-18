package insurance;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

public class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key , Iterator<IntWritable> value , OutputCollector<Text,IntWritable> values, Reporter r) throws IOException
    {
        int freq=0;
        while(value.hasNext())
        {
            freq += value.next().get();

        }
        values.collect(key, new IntWritable(freq));
    }
}