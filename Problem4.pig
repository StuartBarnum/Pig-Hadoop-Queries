register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- load the file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- flatten the subjects out (because group by produces a tuple of each subject
-- in the first column, and we want each object ot be a string, not a tuple),
-- and count the number of tuples associated with each subject
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count_for_subject PARALLEL 50;

-- group the subjects by the count_for_subject column
group_by_counts = group count_by_subject by (count_for_subject) PARALLEL 50;

-- flatten out the elements of group_by_count
count_by_subject_counts = foreach group_by_counts generate flatten($0), COUNT($1) as count_for_subject_count PARALLEL 50;

-- store the histogram values on the cluster
store count_by_subject_counts INTO '/user/hadoop/Problem4-results' using PigStorage();

-- group for the count of the all of the (x,y) values in the histogram
count_by_subject_counts_all = GROUP count_by_subject_counts ALL PARALLEL 50;

-- count the number of (x,y) values in the histogram
total_histogram_elements = FOREACH count_by_subject_counts_all GENERATE COUNT(count_by_subject_counts) PARALLEL 50;

-- store in the cluster the number of (x,y) values in the histgram
store total_histogram_elements INTO '/user/hadoop/Problem4-count' using PigStorage();

