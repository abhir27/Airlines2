
import java.io.IOException;
import java.util.Date;
import java.util.EmptyStackException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

public class FreqMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static long sfmap= new Date().getTime();
	public static long efmap= new Date().getTime();
	public static IntWritable i=new IntWritable(0);
	public static IntWritable one=new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException, EmptyStackException {
    	String[] words=value.toString().split(",");
    	
    	String year=words[0];
    	String month=words[1];
    	String depdelay=words[15];//departure delay
    	String arrdelay=words[14];//Arrival delay
    	String flight=words[9];//flight number
    	output.write(new Text(year+"\t"+month+"\t"+flight), new Text(arrdelay+"\t"+depdelay));
    	efmap= new Date().getTime();
    }
}