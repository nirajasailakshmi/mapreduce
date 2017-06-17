package udfhive;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.io.IntWritable;
//counting no of words in a string or line
public class wordcounthive extends UDF{
public int evaluate(Text text)
{
	int count=0;
	if(text==null) return 0;
	
	StringTokenizer itr = new StringTokenizer(text.toString());
	while(itr.hasMoreTokens())
	{
		count++;
		itr.nextToken();
	}
	
	return count;
}
}
