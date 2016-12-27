package NaiveBayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MatchFactsMap1 extends Mapper<LongWritable,Text,Text,NullWritable>{
		Map<String,String> homeGround=new HashMap<String,String>();
		String fileName;
		String stationName;
		

	protected void setup(Context context) throws IOException{
		try{
			Path[] localFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path eachPath:localFiles){
				fileName=eachPath.getName().toString().trim();
				if(fileName.equals("HomeGrounds.txt")){
					initHomeGrounds(new File("HomeGrounds.txt"));
					break;
					
				}
			}
		}
		catch(NullPointerException e){
			System.out.println("Exception :"+e);
		}
	}
	
	public void initHomeGrounds(File fs) throws IOException
	{
		String line;
		FileReader fr = new FileReader(fs);
		BufferedReader buff = new BufferedReader(fr);
		while((line = buff.readLine()) != null)
		{
			String s[] =line.split(",");
			String key = s[0];
			String value = s[1];
				if (key !=" " && value != " "){
					homeGround.put(key, value);
				}			
		}		
	}	

		
		public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
			String line=value.toString();
			String[] input=line.split(",");
			StringBuffer team1=new StringBuffer();
			StringBuffer team2=new StringBuffer();
			team1.append(input[0]+",");
			team2.append(input[0]+",");
			team1.append(input[4]);
			team2.append(input[5]);
			team1.append(",");
			team2.append(",");
			team1.append(input[5]);
			team2.append(input[4]);
			team1.append(",");
			team2.append(",");
			if (input[2].equals(homeGround.get(input[4]))){
				team1.append("yes,");
				team2.append("no,");
			}
			else if (input[2].equals(homeGround.get(input[5]))){
				team1.append("no,");
				team2.append("yes,");
			}
			else{
				team1.append("no,");
				team2.append("no,");
			}
			
			if (input[4].equals(input[6])){
				team1.append("won,");
				team2.append("loose,");
				if (input[7].equals("bat")){
					team1.append("bat,");
					team2.append("field,");
				}
			}
			else{
				team1.append("loose,");
				team2.append("won,");
				if (input[7].equals("field")){
					team1.append("field,");
					team2.append("bat,");
				}
			}
			if (input[4].equals(input[10])){
				team1.append("won");
				team2.append("loose");
			}
			else if(input[5].equals(input[10])){
				team1.append("loose");
				team2.append("won");
			}
			
			context.write(new Text(team1.toString()), NullWritable.get());
			context.write(new Text(team2.toString()), NullWritable.get());
			
		}
	}
	
