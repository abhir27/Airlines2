

//import java.io.File;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Airlines2 extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
    	System.out.println("Started");
    	int res = ToolRunner.run(new Configuration(), new Airlines2(), args);
        System.exit(res);       
    }
   
    @Override
    public int run(String[] args) throws Exception {
    	System.out.println("in run");
    
        if (args.length != 3) {
            System.out.println("usage: [input] [output] [output1] .");
            System.exit(-1);
        }
        
       /* File dir = new File(args[0]);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
          for (File child : directoryListing) {
            FileSplitter.split(args[0]+"/"+child);
            System.out.println("File splitted: "+args[0]+"/"+child);
          }
        }*/
    	Configuration conf=new Configuration();
        long start= new Date().getTime();
        Job job = Job.getInstance(conf);
        //job.setNumMapTasks(0);
        //job.setNumMapTasks(300); 
        //conf.setInt("mapred.map.tasks", 150);
        //conf.set("mapred.max.split.size", "10*1024");
        //job.setNumReduceTasks(50);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(FreqMapper.class);
        job.setReducerClass(FreqReducer.class);
         job.setInputFormatClass(NLineInputFormat.class);
         NLineInputFormat.addInputPath(job, new Path(args[0]));
         job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 5000000);
        
        //job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(Airlines2.class);
        job.waitForCompletion(true);
        long end= new Date().getTime();
        System.out.println("Time for mapping:"+(FreqMapper.efmap-FreqMapper.sfmap));
        System.out.println("Time for reducing:"+(FreqReducer.efred-FreqReducer.sfred));
        System.out.println("Time for job completion:"+(end-start));
        
        
        long start2= new Date().getTime();
        Job job2 = Job.getInstance(conf);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(ComparisionMapper.class);
        job2.setReducerClass(ComparisionReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
       FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setJarByClass(Airlines2.class);
        job2.waitForCompletion(true);
        long end2= new Date().getTime();
        System.out.println("Time for mapping in job2:"+(ComparisionMapper.efmap-ComparisionMapper.sfmap));
        System.out.println("Time for reducing in job2:"+(ComparisionReducer.efred-ComparisionReducer.sfred));
        System.out.println("Time for job completion in job2: "+(end2-start2));
        
        System.out.println("run end");
        return 0;
    }
}