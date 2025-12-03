# Unemployed Listening

A MapReduce project that correlates yearly genre prevalence in the Million Song Dataset with U.S. unemployment rates.

Authors: Kyle Dickey, Troy Land, Christian Calderon

## Datasets

- [Million Song Dataset](http://millionsongdataset.com/sites/default/files/AdditionalFiles/tracks_per_year.txt) - For mapping genres to years
- [Unemployment Data](https://data.bls.gov/timeseries/LNS14000000) - For unemployment rates by year
- [Genre Annotations](https://www.tagtraum.com/msd_genre_datasets.html) - For genre annotations (specifically the msd_tagtraum_cd2c.cls file)

**Important:**

- The genre annotations file is a .cls file with comments. This program expects the file to be a .txt. You can easily convert the file by just renaming the .cls to .txt.
- The unemployment data file is a .txt file. It has a header row that either needs to be removed or turned into a comment (by placing a `#` at the beginning of the row).

## Building

Requires Java 11+ and Maven.

```bash
mvn clean package
```

This produces `target/unemployed-listening-1.0-SNAPSHOT.jar`.

## Usage

### 1. Run the MapReduce Pipeline

The main driver runs a 3-stage MapReduce pipeline.

Arguments:

- `msd_input`: Path to `msd.txt` (Million Song Dataset tracks per year)
- `genre_input`: Path to `genres.txt` (Genre annotations)
- `unemployment_input`: Path to `unemployment.txt` (BLS unemployment data)
- `output_dir`: Output directory for results

#### Local Mode (Default)

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.driver.UnemployedListeningDriver \
    data/msd.txt data/genres.txt data/unemployment.txt output
```

#### YARN Mode (Cluster)

Runs on a Hadoop cluster using HDFS. Required for large-scale processing.

First, upload data to HDFS:

```bash
hdfs dfs -mkdir -p /user/$(whoami)/unemployed-listening/input
hdfs dfs -put data/msd.txt /user/$(whoami)/unemployed-listening/input/
hdfs dfs -put data/genres.txt /user/$(whoami)/unemployed-listening/input/
hdfs dfs -put data/unemployment.txt /user/$(whoami)/unemployed-listening/input/
```

Then run the job:

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.driver.UnemployedListeningDriver \
    /user/$(whoami)/unemployed-listening/input/msd.txt \
    /user/$(whoami)/unemployed-listening/input/genres.txt \
    /user/$(whoami)/unemployed-listening/input/unemployment.txt \
    /user/$(whoami)/unemployed-listening/output
```

To retrieve results from HDFS:

```bash
hdfs dfs -get /user/$(whoami)/unemployed-listening/output/final ./output
```

Output structure:

```
output/
  intermediate/
    job1_joined/    # year-genre pairs
    job2_counts/    # genre counts per year
  final/            # year, genre, count, unemployment_rate
```

### 2. Analyze Correlations

After the MapReduce pipeline completes, run the correlation analyzer to compute Pearson correlations.

Note: The analyzer and predictor are not MapReduce jobs - they read files using the Hadoop FileSystem API.

#### Local Mode

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.analysis.CorrelationAnalyzer \
    output/final output/correlations.txt
```

#### YARN Mode (Cluster)

On a Hadoop cluster, HDFS is the default filesystem:

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.analysis.CorrelationAnalyzer \
    /user/$(whoami)/unemployed-listening/output/final \
    /user/$(whoami)/unemployed-listening/output/correlations.txt
```

### 3. Predict Genre Trends

Use historical correlations to predict genre trends based on unemployment changes.

Arguments: `<correlation_file> <prev_unemployment> <curr_unemployment>`

#### Local Mode

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.analysis.GenrePredictor \
    output/correlations.txt 4.0 5.5
```

#### YARN Mode (Cluster)

```bash
hadoop jar target/unemployed-listening-1.0-SNAPSHOT.jar \
    com.unemployedlistening.analysis.GenrePredictor \
    /user/$(whoami)/unemployed-listening/output/correlations.txt 4.0 5.5
```

The above examples predict trends when unemployment rises from 4.0% to 5.5%.

## Pipeline Architecture

### Stage 1: Join MSD with Genres (Reduce-Side Join)

- Joins `msd.txt` and `genres.txt` on TrackID
- Filters songs before 1948 (no unemployment data available)
- Output: `year\tgenre`

### Stage 2: Count Genres Per Year

- Aggregates genre occurrences by year
- Uses combiner for efficiency
- Output: `year\tgenre\tcount`

### Stage 3: Merge with Unemployment

- Map-side join with unemployment data (loaded into memory)
- Calculates annual average from monthly rates
- Output: `year\tgenre\tcount\tunemployment_rate`

## Output Format

Final MapReduce output (tab-separated):

```
year    genre    count    unemployment_rate
1960    Rock     1234     5.54
1960    Pop      987      5.54
...
```

Correlation analysis output (tab-separated):

```
Genre    Pearson_Correlation    Data_Points    Avg_Unemployment_Delta    Avg_Count_Delta
Rock     0.2345                 50             0.12                      45.67
...
```

## References

Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere.
The Million Song Dataset. In Proceedings of the 12th International Society
for Music Information Retrieval Conference (ISMIR 2011), 2011.

Hendrik Schreiber. Improving Genre Annotations for the Million Song Dataset. In Proceedings of the 16th International Society for Music Information Retrieval Conference (ISMIR), pages 241-247, Malaga, Spain, Oct. 2015.

U.S. Bureau of Labor Statistics. Labor Force Statistics from the Current Population Survey. U.S. Bureau of Labor Statistics, 2025.
