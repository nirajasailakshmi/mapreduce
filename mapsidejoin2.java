import java.io.BufferedReader;

import java.io.FileReader;

import java.io.IOException;

import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.net.URI;

import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapsidejoin2{

public class myMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
HashMap<String, String> hm= new HashMap<>();
	
public void setup(Context c) throws IOException{
		
Path[] allFiles = DistributedCache.getLocalCacheFiles(c.getConfiguration());		
		
for(Path eachFile : allFiles){
			
if(eachFile.getName().equals("file1")){
				
FileReader fr = new FileReader(eachFile.toString());
				
BufferedReader br = new BufferedReader(fr);
				
String line =br.readLine();
				
while(line != null){
					
String[] eachVal = line.split(" ");
					
String id = eachVal[0];
					
String name = eachVal[1];
					
hm.put(id, name);
					
line=br.readLine();
				
}
				
br.close();
			
}
			
if (hm.isEmpty()) 
			
{
				
throw new IOException("Unable To Load file1");
			
}
		
}
	
}
public void map(LongWritable mInpKey, Text mInpVal, Context c) throws IOException, InterruptedException{
		
String line = mInpVal.toString();
		
String eachVal[] = line.split(" ");
		
String id=eachVal[0];
		
String amt= eachVal[1];
		
String name = hm.get(id);
		
		
Text mOutKey = new Text("User ID : " +id +" Name : " +name);
		
DoubleWritable mOutVal = new DoubleWritable(Double.parseDouble(amt));
		
		
c.write(mOutKey, mOutVal);
		
	
} 
public class myReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
public void reduce(Text rInpKey, Iterable<DoubleWritable> rInpVal,Context c ) throws IOException, InterruptedException{
		
double amt=0.0;
		
for(DoubleWritable each: rInpVal){
			
amt+= Double.parseDouble(each.toString());
		
}
		
c.write(rInpKey, new Text(" Amount : " +amt));
	
}


}

public  class myDriver  {
	
@SuppressWarnings("deprecation")
public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
	
Job j = new Job(new Configuration(), "Mapper Side Join/ In Memory Join");
	
j.setJarByClass(myDriver.class);
	
j.setMapperClass(myMapper.class);
	
j.setReducerClass(myReducer.class);
	
j.setNumReduceTasks(1);
	
j.setMapOutputKeyClass(Text.class);
	
j.setMapOutputValueClass(DoubleWritable.class);
	
	
FileInputFormat.addInputPath(j,new Path(args[0]));
	
FileOutputFormat.setOutputPath(j, new Path(args[1]));
	
	
DistributedCache.addCacheFile(new URI("/file1"),j.getConfiguration());

System.exit(j.waitForCompletion(true)?0:1);
	
}
}
}
}

