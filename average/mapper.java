package avg;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;
import java.io.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable>{
    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> values, Reporter r) throws IOException{
        String word[] = value.toString().split("\\t");
        String sex = word[3];
        float salary = Float.parseFloat(word[8]);
        values.collect(new Text(sex), new FloatWritable(salary));
    }
}