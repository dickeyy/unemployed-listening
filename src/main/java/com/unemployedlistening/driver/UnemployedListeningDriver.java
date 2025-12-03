package com.unemployedlistening.driver;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.unemployedlistening.job1.GenreMapper;
import com.unemployedlistening.job1.JoinReducer;
import com.unemployedlistening.job1.MsdMapper;
import com.unemployedlistening.job2.GenreCountMapper;
import com.unemployedlistening.job2.GenreCountReducer;
import com.unemployedlistening.job3.UnemploymentJoinMapper;

/**
 * Main driver class that orchestrates the 3-stage MapReduce pipeline:
 * 1. Join MSD with genre annotations by track ID
 * 2. Count genre occurrences per year
 * 3. Merge with unemployment data
 */
public class UnemployedListeningDriver extends Configured implements Tool {

    private static final String JOB1_OUTPUT = "intermediate/job1_joined";
    private static final String JOB2_OUTPUT = "intermediate/job2_counts";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println(
                    "Usage: UnemployedListeningDriver <msd_input> <genre_input> <unemployment_input> <output>");
            System.err.println("  msd_input: Path to the Million Song Dataset file (msd.txt)");
            System.err.println("  genre_input: Path to the genre annotations file (genres.txt)");
            System.err.println("  unemployment_input: Path to the unemployment data file (unemployment.txt)");
            System.err.println("  output: Output directory for final results");
            return 1;
        }

        String msdInput = args[0];
        String genreInput = args[1];
        String unemploymentInput = args[2];
        String finalOutput = args[3];

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Create intermediate output paths
        Path job1OutputPath = new Path(finalOutput, JOB1_OUTPUT);
        Path job2OutputPath = new Path(finalOutput, JOB2_OUTPUT);
        Path finalOutputPath = new Path(finalOutput, "final");

        // Clean up intermediate directories if they exist
        if (fs.exists(job1OutputPath)) {
            fs.delete(job1OutputPath, true);
        }
        if (fs.exists(job2OutputPath)) {
            fs.delete(job2OutputPath, true);
        }
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        // Run Job 1: Join MSD with Genres
        System.out.println("Starting Job 1: Joining MSD with Genre annotations...");
        if (!runJob1(conf, msdInput, genreInput, job1OutputPath)) {
            System.err.println("Job 1 failed!");
            return 1;
        }
        System.out.println("Job 1 completed successfully.");

        // Run Job 2: Count Genres per Year
        System.out.println("Starting Job 2: Counting genres per year...");
        if (!runJob2(conf, job1OutputPath, job2OutputPath)) {
            System.err.println("Job 2 failed!");
            return 1;
        }
        System.out.println("Job 2 completed successfully.");

        // Run Job 3: Merge with Unemployment Data
        System.out.println("Starting Job 3: Merging with unemployment data...");
        if (!runJob3(conf, job2OutputPath, unemploymentInput, finalOutputPath)) {
            System.err.println("Job 3 failed!");
            return 1;
        }
        System.out.println("Job 3 completed successfully.");

        System.out.println("All jobs completed. Output written to: " + finalOutputPath);
        return 0;
    }

    // Job 1: Join MSD data with genre annotations using reduce-side join.
    private boolean runJob1(Configuration conf, String msdInput, String genreInput, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Unemployed Listening - Job 1: Join MSD with Genres");
        job.setJarByClass(UnemployedListeningDriver.class);

        // Use MultipleInputs for different input formats
        MultipleInputs.addInputPath(job, new Path(msdInput), TextInputFormat.class, MsdMapper.class);
        MultipleInputs.addInputPath(job, new Path(genreInput), TextInputFormat.class, GenreMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    // Job 2: Count genre occurrences per year.
    private boolean runJob2(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Unemployed Listening - Job 2: Count Genres per Year");
        job.setJarByClass(UnemployedListeningDriver.class);

        job.setMapperClass(GenreCountMapper.class);
        job.setCombinerClass(GenreCountCombiner.class);
        job.setReducerClass(GenreCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    // Job 3: Map-side join with unemployment data.
    private boolean runJob3(Configuration conf, Path inputPath, String unemploymentInput, Path outputPath)
            throws Exception {
        Job job = Job.getInstance(conf, "Unemployed Listening - Job 3: Merge with Unemployment");
        job.setJarByClass(UnemployedListeningDriver.class);

        // Add unemployment data to distributed cache
        job.addCacheFile(new URI(unemploymentInput));

        job.setMapperClass(UnemploymentJoinMapper.class);
        job.setNumReduceTasks(0); // Map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    // Inner combiner class for Job 2 to reduce data shuffled.
    public static class GenreCountCombiner
            extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws java.io.IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new UnemployedListeningDriver(), args);
        System.exit(exitCode);
    }
}
