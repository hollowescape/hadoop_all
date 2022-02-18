package insurance;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.util.*;
import java.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text, Text, IntWritable>
{
    public void map(LongWritable key , Text value, OutputCollector<Text,IntWritable> values, Reporter r)  throws IOException 
    {
            String[] valueString = value.toString().split(",");            
            values.collect(new Text(valueString[16]),new IntWritable(1));
    }
}

