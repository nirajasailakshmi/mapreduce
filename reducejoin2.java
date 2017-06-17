import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class reducejoin2 {
	public  class productMapper extends Mapper <Object,Text, Text,Text>
	 {
	 public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
	 {
	 String record = value.toString();
	 String[] parts= record.split(",");
	 context.write(new Text(parts[0]), new Text("purchase\t"+parts[1]));
	 }
	 }
	public class salesMapper extends Mapper <Object, Text,Text,Text>
	 {
	 public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException 
	 {
	 String record = value.toString();
	 String[] parts = record.split(",");
	
	 context.write(new Text(parts[0]), new Text("sales\t"+parts[1]));
	 }
	 }
	public static class ReduceJoinReducer extends
	Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
	double total = 0.0;
	double total1 = 0.0;
	for (Text t : values) {
		String parts[] = t.toString().split("\t");
		if (parts[0].equals("purchase")) {
			total += Float.parseFloat(parts[1]);
		} else if (parts[0].equals("sales")) {
			total1 += Float.parseFloat(parts[1]);
		}
	}
	String str = String.format("%f\t%f", total1, total);
	context.write(new Text(key), new Text(str));
}
}

@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "Purchase_Sales join");
job.setJarByClass(reducejoin2.class);
job.setReducerClass(ReduceJoinReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);


MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, productMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, salesMapper.class);
Path outputPath = new Path(args[2]);


FileOutputFormat.setOutputPath(job, outputPath);
//outputPath.getFileSystem(conf).delete(outputPath);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}

	 