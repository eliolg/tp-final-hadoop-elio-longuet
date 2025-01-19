package org.epf.hadoop.colfil2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job2 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ColFilJob1");

        job.setJarByClass(ColFilJob2.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(UserPairPartitioner.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}