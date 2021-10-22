pages = LOAD 'gs://pagerank/crawl.csv' using PigStorage(';') AS (site1:chararray, site2:chararray);
groups = GROUP pages BY site1;
init = FOREACH groups GENERATE group, 1, COUNT(pages);
STORE init INTO 'gs://pagerank/pagerank_init' using PigStorage(';');