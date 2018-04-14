import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by colntrev on 2/18/18.
 */
public class TriangleCounting {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String filePath = 
        Job job1, job2, job3; // we need three different jobs for this task
        String edgeOutput = "/triangles/edgeResult";
        String triangleOutput = "/triangles/triangleResult";
        String finalOutput = "/triangles/finalOutput";
        Configuration conf = new Configuration();

        job1 = Job.getInstance(conf);
        job1.setJobName("Edge Reader");
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(EdgePair.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setJarByClass(TriangleCounting.class);
        job1.setMapperClass(GraphEdgeMap.class);
        job1.setReducerClass(GraphEdgeReduce.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(edgeOutput));

        job2 = Job.getInstance(conf);
        job2.setJobName("Triangle Finder");
        job2.setMapOutputKeyClass(EdgePair.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setJarByClass(TriangleCounting.class);
        job2.setMapperClass(TriangleMap.class);
        job2.setReducerClass(TriangleReduce.class);
        FileInputFormat.addInputPath(job2, new Path(edgeOutput));
        FileOutputFormat.setOutputPath(job2, new Path(triangleOutput));

        job3 = Job.getInstance(conf);
        job3.setJobName("Edge Reader");
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setJarByClass(TriangleCounting.class);
        job3.setMapperClass(UniqueTriangleMap.class);
        job3.setReducerClass(UniqueTriangleReduce.class);
        FileInputFormat.addInputPath(job3, new Path(triangleOutput));
        FileOutputFormat.setOutputPath(job3, new Path(finalOutput));

        long startTime = System.currentTimeMillis();
        int result = runJobs(job1,job2,job3);
        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed Time: " + (endTime - startTime));
        System.exit(result);
    }

    public static int runJobs(Job j1, Job j2, Job j3) throws InterruptedException, IOException, ClassNotFoundException {
        int status = j1.waitForCompletion(true)? 0 : 1;

        if(status == 0){
            status = j2.waitForCompletion(true)? 0 : 1;
        }

        if(status == 0){
            status = j3.waitForCompletion(true)? 0 : 1;
        }
        return status;
    }

}
