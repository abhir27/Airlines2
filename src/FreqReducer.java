

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FreqReducer extends Reducer<Text, Text,Text, Text > {
	public static long sfred= new Date().getTime();
	public static long efred= new Date().getTime();
	public static int size=0;
	@Override
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
        int adelay=0,ddelay=0,count=0;
    	for(Text val:values)
    	{
    		String[] s=val.toString().split("\t");
    		if(!s[0].equals("NA") && !s[0].equals("ArrDelay"))
    			adelay=adelay+Integer.parseInt(s[0]);
    		
    		if(!s[1].equals("NA") && !s[1].equals("DepDelay"))
    			ddelay=ddelay+Integer.parseInt(s[1]);
    		count++;
    	}
    	adelay=adelay/count;
    	ddelay=ddelay/count;
        output.write(key, new Text(adelay+"\t"+ddelay));
        efred= new Date().getTime();
    }
}