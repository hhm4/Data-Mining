package NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ValidateReduce4 extends Reducer<Text,LongWritable,Text,LongWritable>{
	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
		int count=0;
		for(LongWritable val:values){
			count=count+1;
		}
		context.write(key, new LongWritable(count));
	}
	
}	