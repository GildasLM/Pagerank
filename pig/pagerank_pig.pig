pages = LOAD 'gs://pagerank/crawl.csv' using PigStorage(';') AS (site1:chararray, site2:chararray);
groups = GROUP pages BY site1;
init = FOREACH groups GENERATE group, 1, COUNT(pages);
STORE init INTO 'gs://pagerank/pagerank_init' using PigStorage(';');

links = LOAD 'gs://pagerank/crawl.csv' using PigStorage(';') AS (url:chararray, target:chararray);
ranks = LOAD 'gs://pagerank/pagerank_init/*' using PigStorage(';') AS (site:chararray, p_rank:float, total_links:int);
pages = GROUP links by url;
pages = JOIN pages by group, ranks by site;
processed_link = FOREACH pages GENERATE FLATTEN(links.target), p_rank / total_links AS pagerank;
grouped = COGROUP processed_link BY target, ranks by site;
result = FOREACH grouped GENERATE
	group AS id, (IsEmpty(processed_link) ? 0.15 : 0.15 + 0.85 * SUM(processed_link.pagerank)) AS newRank, FLATTEN(ranks.total_links);
STORE result INTO 'gs://pagerank/output_pagerank_pig' using PigStorage(';');