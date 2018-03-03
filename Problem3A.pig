register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- load the file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples1 = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject1:chararray,predicate1:chararray,object1:chararray);

-- select the ntriples that have a match with 'rdfabout.com' in the subject
ntriples1_filtered = FILTER ntriples1 BY (subject1 matches '.*business.*');

-- parse each line into ntriples
ntriples2 = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject2:chararray,predicate2:chararray,object2:chararray);

-- select the ntriples that have a match with 'rdfabout.com' in the subject
ntriples2_filtered = FILTER ntriples2 BY (subject2 matches '.*business.*');

-- for this test run (for the larger dataset in Problem3B), we join by subject1 = subject2
joined_triples = join ntriples1_filtered by subject1, ntriples2_filtered by subject2;

-- dispose of duplicates
length_2_chains = DISTINCT joined_triples;

-- store the results on the cluster
store length_2_chains INTO '/user/hadoop/Problem3A' using PigStorage();

