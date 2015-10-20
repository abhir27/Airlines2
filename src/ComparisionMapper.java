
import java.io.IOException;
import java.util.Date;
import java.util.EmptyStackException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

public class ComparisionMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static long sfmap= new Date().getTime();
	public static long efmap= new Date().getTime();
	public static IntWritable i=new IntWritable(0);
	public static IntWritable one=new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException, EmptyStackException {
    	String[] words=value.toString().split("\t");
    	output.write(new Text(words[2] +"\t"+words[1]),new Text(words[0]+"\t"+words[3]+"\t"+words[4]) );
    	efmap= new Date().getTime();
    }
}