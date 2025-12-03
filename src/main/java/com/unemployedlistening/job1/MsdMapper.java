package com.unemployedlistening.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.unemployedlistening.util.UnemploymentLoader;

/**
 * Mapper for the Million Song Dataset file.
 * Parses the MSD format (YEAR<SEP>TRACKID<SEP>ARTIST<SEP>SONG) and emits
 * (trackId, "MSD|year").
 * Filters out songs from before 1948 (earliest unemployment data).
 */
public class MsdMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final String DELIMITER = "<SEP>";
    private static final String SOURCE_TAG = "MSD|";

    private Text trackIdKey = new Text();
    private Text yearValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split(DELIMITER);

        // Expected format: YEAR<SEP>TRACKID<SEP>ARTIST<SEP>SONG
        if (parts.length < 2) {
            return;
        }

        try {
            int year = Integer.parseInt(parts[0].trim());
            String trackId = parts[1].trim();

            // Filter out songs from before we have unemployment data
            if (year < UnemploymentLoader.EARLIEST_YEAR) {
                return;
            }

            if (trackId.isEmpty()) {
                return;
            }

            trackIdKey.set(trackId);
            yearValue.set(SOURCE_TAG + year);

            context.write(trackIdKey, yearValue);

        } catch (NumberFormatException e) {
            // Skip malformed lines
            return;
        }
    }
}
