package com.unemployedlistening.job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer that performs a reduce-side join between MSD data and genre annotations.
 * Input: (trackId, [list of "MSD|year" and "GENRE|genre" values])
 * Output: (year\tgenre, null) for each successful join
 */
public class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    
    private static final String MSD_TAG = "MSD|";
    private static final String GENRE_TAG = "GENRE|";
    
    private Text outputKey = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> years = new ArrayList<>();
        List<String> genres = new ArrayList<>();
        
        // Separate MSD records (years) from genre records
        for (Text value : values) {
            String val = value.toString();
            
            if (val.startsWith(MSD_TAG)) {
                years.add(val.substring(MSD_TAG.length()));
            } else if (val.startsWith(GENRE_TAG)) {
                genres.add(val.substring(GENRE_TAG.length()));
            }
        }
        
        // Only emit if we have both year data and genre data for this track
        if (years.isEmpty() || genres.isEmpty()) {
            return;
        }
        
        // Emit all combinations of year and genre for this track
        // (typically there should be one year per track, but could have multiple genres)
        for (String year : years) {
            for (String genre : genres) {
                outputKey.set(year + "\t" + genre);
                context.write(outputKey, NullWritable.get());
            }
        }
    }
}

