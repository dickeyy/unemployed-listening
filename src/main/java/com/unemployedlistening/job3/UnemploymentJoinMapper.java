package com.unemployedlistening.job3;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.unemployedlistening.util.UnemploymentLoader;

/**
 * Mapper that joins genre count data with unemployment rates.
 * Uses a map-side join by loading unemployment data into memory during setup.
 * Input: year\tgenre\tcount (output from Job 2)
 * Output: year\tgenre\tcount\tunemployment_rate
 */
public class UnemploymentJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<Integer, Double> unemploymentData;
    private Text outputKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        // Load unemployment data from distributed cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path unemploymentPath = new Path(cacheFiles[0].getPath());
            unemploymentData = UnemploymentLoader.loadUnemploymentData(unemploymentPath, conf);
        } else {
            // Fall back to configuration path
            String unemploymentPathStr = conf.get("unemployment.data.path");
            if (unemploymentPathStr != null) {
                unemploymentData = UnemploymentLoader.loadUnemploymentData(unemploymentPathStr, conf);
            } else {
                throw new IOException("Unemployment data file not found in distributed cache or configuration");
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        // Input format: year\tgenre\tcount
        String[] parts = line.split("\t");

        if (parts.length < 3) {
            return;
        }

        try {
            int year = Integer.parseInt(parts[0].trim());
            String genre = parts[1].trim();
            int count = Integer.parseInt(parts[2].trim());

            // Look up unemployment rate for this year
            Double unemploymentRate = unemploymentData.get(year);

            if (unemploymentRate == null) {
                // Skip years without unemployment data
                return;
            }

            // Output format: year\tgenre\tcount\tunemployment_rate
            outputKey.set(String.format("%d\t%s\t%d\t%.2f", year, genre, count, unemploymentRate));
            context.write(outputKey, NullWritable.get());

        } catch (NumberFormatException e) {
            // Skip malformed lines
            return;
        }
    }
}
