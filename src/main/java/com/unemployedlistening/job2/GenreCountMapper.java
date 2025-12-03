package com.unemployedlistening.job2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for counting genre occurrences per year.
 * Input: year\tgenre (output from Job 1)
 * Output: (year\tgenre, 1)
 */
public class GenreCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text compositeKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        // Input format: year\tgenre
        String[] parts = line.split("\t");

        if (parts.length < 2) {
            return;
        }

        String year = parts[0].trim();
        String genre = parts[1].trim();

        if (year.isEmpty() || genre.isEmpty()) {
            return;
        }

        // Use year\tgenre as composite key
        compositeKey.set(year + "\t" + genre);
        context.write(compositeKey, ONE);
    }
}
