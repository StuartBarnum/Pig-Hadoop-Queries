register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);

-- later you will load to other files, example:
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- flatten the subjects out (because group by produces a tuple of each subject
-- in the first column, and we want each object ot be a string, not a tuple),
-- and count the number of tuples associated with each object
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count_for_subject PARALLEL 50;

-- group the elements of count_by_subject by count_for_subect
group_by_counts = group count_by_subject by (count_for_subject) PARALLEL 50;

-- count within each of the count_by_subject groups, to obtain the number of (x,y) value-pairs in the histogram
count_by_subject_counts = foreach group_by_counts generate flatten($0), COUNT($1) as count_for_subject_count PARALLEL 50;

-- store on histogram values on the cluseter
store count_by_subject_counts INTO '/user/hadoop/Problem2A-results' using PigStorage();

-- group for a count of the number of (x,y) pairs in the histogram
count_by_subject_counts_all = GROUP count_by_subject_counts ALL;

-- count the number of (x,y) pairs in the histogram
total_histogram_elements = FOREACH count_by_subject_counts_all GENERATE COUNT(count_by_subject_counts);

-- store the final count, the number we need, on the cluster
store total_histogram_elements INTO '/user/hadoop/Problem2A-count' using PigStorage();


