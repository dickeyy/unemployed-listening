package com.unemployedlistening.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Analyzes the output from the MapReduce pipeline to calculate correlations
 * between genre prevalence changes and unemployment rate changes.
 * 
 * Input format: year\tgenre\tcount\tunemployment_rate
 * Output: Pearson correlation coefficients for each genre
 */
public class CorrelationAnalyzer extends Configured implements Tool {

    // Data structure to hold year-genre-count-unemployment records
    private static class GenreYearData {
        int year;
        String genre;
        int count;
        double unemploymentRate;

        GenreYearData(int year, String genre, int count, double unemploymentRate) {
            this.year = year;
            this.genre = genre;
            this.count = count;
            this.unemploymentRate = unemploymentRate;
        }
    }

    // Result of correlation analysis for a genre
    public static class CorrelationResult {
        String genre;
        double pearsonCorrelation;
        int dataPoints;
        double avgUnemploymentChange;
        double avgCountChange;

        CorrelationResult(String genre, double pearsonCorrelation, int dataPoints,
                double avgUnemploymentChange, double avgCountChange) {
            this.genre = genre;
            this.pearsonCorrelation = pearsonCorrelation;
            this.dataPoints = dataPoints;
            this.avgUnemploymentChange = avgUnemploymentChange;
            this.avgCountChange = avgCountChange;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CorrelationAnalyzer <input_dir> <output_file>");
            System.err.println(
                    "  input_dir: Directory containing MapReduce output (year\\tgenre\\tcount\\tunemployment_rate)");
            System.err.println("  output_file: Output file for correlation analysis results");
            return 1;
        }

        String inputDir = args[0];
        String outputFile = args[1];

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Load all data from input directory
        System.out.println("Loading data from: " + inputDir);
        List<GenreYearData> allData = loadData(fs, new Path(inputDir));
        System.out.println("Loaded " + allData.size() + " records.");

        // Organize data by genre
        Map<String, TreeMap<Integer, GenreYearData>> dataByGenre = organizeByGenre(allData);
        System.out.println("Found " + dataByGenre.size() + " unique genres.");

        // Calculate correlations for each genre
        List<CorrelationResult> results = calculateCorrelations(dataByGenre);

        // Write results
        writeResults(fs, new Path(outputFile), results);

        // Print summary
        printSummary(results);

        return 0;
    }

    // Load data from all part files in the input directory
    private List<GenreYearData> loadData(FileSystem fs, Path inputDir) throws IOException {
        List<GenreYearData> data = new ArrayList<>();

        FileStatus[] files = fs.listStatus(inputDir);
        for (FileStatus file : files) {
            if (file.isFile() && file.getPath().getName().startsWith("part-")) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(file.getPath())))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty())
                            continue;

                        String[] parts = line.split("\t");
                        if (parts.length < 4)
                            continue;

                        try {
                            int year = Integer.parseInt(parts[0].trim());
                            String genre = parts[1].trim();
                            int count = Integer.parseInt(parts[2].trim());
                            double unemploymentRate = Double.parseDouble(parts[3].trim());

                            data.add(new GenreYearData(year, genre, count, unemploymentRate));
                        } catch (NumberFormatException e) {
                            // Skip malformed lines
                        }
                    }
                }
            }
        }

        return data;
    }

    // Organize data by genre with years in sorted order
    private Map<String, TreeMap<Integer, GenreYearData>> organizeByGenre(List<GenreYearData> allData) {
        Map<String, TreeMap<Integer, GenreYearData>> dataByGenre = new HashMap<>();

        for (GenreYearData record : allData) {
            dataByGenre.computeIfAbsent(record.genre, k -> new TreeMap<>())
                    .put(record.year, record);
        }

        return dataByGenre;
    }

    // Calculate Pearson correlation coefficient for each genre
    // between year-over-year changes in count and unemployment
    private List<CorrelationResult> calculateCorrelations(
            Map<String, TreeMap<Integer, GenreYearData>> dataByGenre) {

        List<CorrelationResult> results = new ArrayList<>();

        for (Map.Entry<String, TreeMap<Integer, GenreYearData>> entry : dataByGenre.entrySet()) {
            String genre = entry.getKey();
            TreeMap<Integer, GenreYearData> yearData = entry.getValue();

            // Need at least 2 years to calculate deltas
            if (yearData.size() < 2) {
                continue;
            }

            // Calculate year-over-year deltas
            List<Double> unemploymentDeltas = new ArrayList<>();
            List<Double> countDeltas = new ArrayList<>();

            Integer prevYear = null;
            for (Map.Entry<Integer, GenreYearData> yearEntry : yearData.entrySet()) {
                if (prevYear != null) {
                    GenreYearData prev = yearData.get(prevYear);
                    GenreYearData curr = yearEntry.getValue();

                    double unemploymentDelta = curr.unemploymentRate - prev.unemploymentRate;
                    double countDelta = curr.count - prev.count;

                    unemploymentDeltas.add(unemploymentDelta);
                    countDeltas.add(countDelta);
                }
                prevYear = yearEntry.getKey();
            }

            // Calculate Pearson correlation
            if (unemploymentDeltas.size() >= 2) {
                double correlation = calculatePearsonCorrelation(unemploymentDeltas, countDeltas);
                double avgUnemploymentChange = average(unemploymentDeltas);
                double avgCountChange = average(countDeltas);

                results.add(new CorrelationResult(genre, correlation, unemploymentDeltas.size(),
                        avgUnemploymentChange, avgCountChange));
            }
        }

        // Sort by absolute correlation (strongest correlations first)
        results.sort((a, b) -> Double.compare(Math.abs(b.pearsonCorrelation), Math.abs(a.pearsonCorrelation)));

        return results;
    }

    // Calculate Pearson correlation coefficient between two lists
    private double calculatePearsonCorrelation(List<Double> x, List<Double> y) {
        int n = x.size();
        if (n != y.size() || n == 0) {
            return 0.0;
        }

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;

        for (int i = 0; i < n; i++) {
            double xi = x.get(i);
            double yi = y.get(i);
            sumX += xi;
            sumY += yi;
            sumXY += xi * yi;
            sumX2 += xi * xi;
            sumY2 += yi * yi;
        }

        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        if (denominator == 0) {
            return 0.0;
        }

        return numerator / denominator;
    }

    // Calculate average of a list
    private double average(List<Double> values) {
        if (values.isEmpty())
            return 0;
        double sum = 0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.size();
    }

    // Write correlation results to output file
    private void writeResults(FileSystem fs, Path outputPath, List<CorrelationResult> results) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(outputPath, true)))) {

            // Write header
            writer.write("Genre\tPearson_Correlation\tData_Points\tAvg_Unemployment_Delta\tAvg_Count_Delta");
            writer.newLine();

            // Write data
            for (CorrelationResult result : results) {
                writer.write(String.format("%s\t%.4f\t%d\t%.4f\t%.2f",
                        result.genre,
                        result.pearsonCorrelation,
                        result.dataPoints,
                        result.avgUnemploymentChange,
                        result.avgCountChange));
                writer.newLine();
            }
        }

        System.out.println("Results written to: " + outputPath);
    }

    // Print summary of correlation analysis
    private void printSummary(List<CorrelationResult> results) {
        System.out.println("\n=== Correlation Analysis Summary ===\n");

        if (results.isEmpty()) {
            System.out.println("No results to display.");
            return;
        }

        System.out.println("Top 10 Strongest Correlations (by absolute value):");
        System.out.println(String.format("%-20s %12s %12s", "Genre", "Correlation", "Data Points"));
        System.out.println("-".repeat(50));

        int count = 0;
        for (CorrelationResult result : results) {
            if (count >= 10)
                break;

            String interpretation;
            if (result.pearsonCorrelation > 0.5) {
                interpretation = "(strong positive)";
            } else if (result.pearsonCorrelation > 0.2) {
                interpretation = "(weak positive)";
            } else if (result.pearsonCorrelation < -0.5) {
                interpretation = "(strong negative)";
            } else if (result.pearsonCorrelation < -0.2) {
                interpretation = "(weak negative)";
            } else {
                interpretation = "(negligible)";
            }

            System.out.println(String.format("%-20s %12.4f %12d %s",
                    result.genre, result.pearsonCorrelation, result.dataPoints, interpretation));
            count++;
        }

        System.out.println("\nInterpretation:");
        System.out.println("- Positive correlation: Genre count increases when unemployment increases");
        System.out.println("- Negative correlation: Genre count decreases when unemployment increases");
        System.out.println("- Correlation near 0: No linear relationship between unemployment and genre count changes");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CorrelationAnalyzer(), args);
        System.exit(exitCode);
    }
}
