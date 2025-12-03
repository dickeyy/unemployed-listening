package com.unemployedlistening.job2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for counting genre occurrences per year.
 * Input: (year\tgenre, [1, 1, 1, ...])
 * Output: year\tgenre\tcount
 */
public class GenreCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private Text outputKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        // Output format: year\tgenre\tcount
        outputKey.set(key.toString() + "\t" + count);
        context.write(outputKey, NullWritable.get());
    }
}
