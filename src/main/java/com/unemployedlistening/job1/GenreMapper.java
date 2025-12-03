package com.unemployedlistening.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for the Genre annotations file.
 * Parses the genre format (TRACKID\tGENRE) and emits (trackId, "GENRE|genre").
 */
public class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final String SOURCE_TAG = "GENRE|";
    
    private Text trackIdKey = new Text();
    private Text genreValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        if (line.isEmpty() || line.startsWith("#")) {
            return;
        }
        
        // Tab-delimited: TRACKID\tGENRE
        String[] parts = line.split("\t");
        
        if (parts.length < 2) {
            return;
        }
        
        String trackId = parts[0].trim();
        String genre = parts[1].trim();
        
        if (trackId.isEmpty() || genre.isEmpty()) {
            return;
        }
        
        trackIdKey.set(trackId);
        genreValue.set(SOURCE_TAG + genre);
        
        context.write(trackIdKey, genreValue);
    }
}

