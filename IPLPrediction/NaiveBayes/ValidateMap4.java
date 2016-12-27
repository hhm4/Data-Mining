package NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ValidateMap4 extends Mapper<LongWritable,Text,Text,LongWritable>{
	JobConf conf;
	public void configure(JobConf conf) {
	    this.conf = conf;
	}
	
	public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
		String line=value.toString();
		String[] input=line.split(",");
		String wonBy=input[1].substring(6);
		String prediction=input[2].substring(11);
		if (wonBy.equals(prediction)){
			context.write(new Text("Correct"), new LongWritable(1));
		}
		else{
			context.write(new Text("InCorrect"), new LongWritable(1));
		}
	}
}
