

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ComparisionReducer extends Reducer<Text, Text,Text, Text > {
	public static long sfred= new Date().getTime();
	public static long efred= new Date().getTime();
	//public static int size=0;
	@Override
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
		String result="";
        for(Text val:values)
        {
        	String[] s=val.toString().split("\t");
        	result=result+"\t"+s[1]+"\t"+s[2];
        }
        output.write(key, new Text(result));
        efred= new Date().getTime();
    }
}