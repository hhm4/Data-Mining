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

public  class PredictMap3 extends Mapper<LongWritable,Text,Text,Text>{
		Map<String,Float> probability=new HashMap<String,Float>();
		Map<String,String> homeGround=new HashMap<String,String>();
		String fileName;
		String stationName;
		

	protected void setup(Context context) throws IOException{
		try{
			Path[] localFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			Boolean flag1=false;
			Boolean flag2=false;
			for(Path eachPath:localFiles){
				fileName=eachPath.getName().toString().trim();
				if(fileName.equals("condprob-r-00000")){
					initProbability(new File("condprob-r-00000"));
					flag1=true;
				}
				if(fileName.equals("HomeGrounds.txt")){
					initHomeGrounds(new File("HomeGrounds.txt"));
					flag2=true;
				}
				if (flag1 && flag2){
					break;
				}
				
			}
		}
		catch(NullPointerException e){
			System.out.println("Exception :"+e);
		}
	}
	
	public void initProbability(File fs) throws IOException
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
					probability.put(key, Float.valueOf(value));
				}			
		}
		Float won=probability.get("won");
		Float loose=probability.get("loose");
		Float total=won+loose;
		probability.put("won", won/total);
		probability.put("loose", loose/total);
		for (String k:probability.keySet()){
			
			if(!(k.equals("won")||k.equals("loose"))){
				String result=k.split(":")[1];
				if (result.equals("won")){
					probability.put(k,probability.get(k)/won);
				}
				if (result.equals("loose")){
					probability.put(k,probability.get(k)/loose);
				}
			}
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
				team1.append("won");
				team2.append("loose");
				if (input[7].equals("bat")){
					team1.append("bat");
					team2.append("field");
				}
			}
			else{
				team1.append("loose");
				team2.append("won");
				if (input[7].equals("field")){
					team1.append("field");
					team2.append("bat");
				}
			}

			context.write(new Text(input[0].toString()), new Text("*:"+input[4]+":"+input[5]+":"+input[10]));
			String t1[]=team1.toString().split(",");
			String t2[]=team2.toString().split(",");
			Float prob1=Float.valueOf(probability.get("won"));
			Float prob2=Float.valueOf(probability.get("loose"));
			for (int i=1;i<t1.length;i++){
				try{
					prob1=prob1*probability.get(t1[i]+":won");
				}
				catch(NullPointerException e){
					prob1=0.0f;
				}
				try{
					prob2=prob2*probability.get(t1[i]+":loose");
				}
				catch(NullPointerException e){
					prob2=0.0f;
				}
			}
			context.write(new Text(input[0].toString()), new Text(input[4]+":won:"+String.valueOf(prob1)));
			context.write(new Text(input[0].toString()), new Text(input[4]+":loose:"+String.valueOf(prob2)));
			
			prob1=Float.valueOf(probability.get("won"));
			prob2=Float.valueOf(probability.get("loose"));
			for (int i=1;i<t2.length;i++){
				try{
					prob1=prob1*probability.get(t2[i]+":won");
				}
				catch(NullPointerException e){
					prob1=0.0f;
				}
				try{
					prob2=prob2*probability.get(t2[i]+":loose");
				}
				catch(NullPointerException e){
					prob2=0.0f;
				}
			}
			context.write(new Text(input[0].toString()), new Text(input[5]+":won:"+String.valueOf(prob1)));
			context.write(new Text(input[0].toString()), new Text(input[5]+":loose:"+String.valueOf(prob2)));
			
		}
	}	
	

