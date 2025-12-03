package com.unemployedlistening.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to load and parse unemployment data from the BLS dataset.
 * Calculates annual averages from monthly unemployment rates.
 */
public class UnemploymentLoader {

    /**
     * Loads unemployment data from a file and returns a map of year to annual
     * average rate.
     * 
     * @param path Path to the unemployment data file
     * @param conf Hadoop configuration
     * @return Map of year (Integer) to annual average unemployment rate (Double)
     * @throws IOException if file cannot be read
     */
    public static Map<Integer, Double> loadUnemploymentData(Path path, Configuration conf) throws IOException {
        Map<Integer, Double> unemploymentByYear = new HashMap<>();

        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // Skip empty lines and comments
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split(",");
                if (parts.length < 13) {
                    continue;
                }

                try {
                    int year = Integer.parseInt(parts[0].trim());
                    double sum = 0.0;
                    int count = 0;

                    // Parse months 1-12 (indices 1-12)
                    for (int i = 1; i <= 12; i++) {
                        if (i < parts.length) {
                            String monthValue = parts[i].trim();
                            if (!monthValue.isEmpty()) {
                                sum += Double.parseDouble(monthValue);
                                count++;
                            }
                        }
                    }

                    if (count > 0) {
                        double annualAverage = sum / count;
                        unemploymentByYear.put(year, annualAverage);
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed lines
                    continue;
                }
            }
        }

        return unemploymentByYear;
    }

    /**
     * Loads unemployment data from a local file path string.
     * 
     * @param pathString Path string to the unemployment data file
     * @param conf       Hadoop configuration
     * @return Map of year (Integer) to annual average unemployment rate (Double)
     * @throws IOException if file cannot be read
     */
    public static Map<Integer, Double> loadUnemploymentData(String pathString, Configuration conf) throws IOException {
        return loadUnemploymentData(new Path(pathString), conf);
    }

    // The earliest year for which we have unemployment data.
    public static final int EARLIEST_YEAR = 1948;
}
