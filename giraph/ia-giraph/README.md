To build (for Hadoop 0.20.2): mvn -Phadoop_non_secure install

hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.OutDegreeCountVertex -vif org.archive.giraph.LongDoubleFloatTextInputFormat -vip /user/vinay/tab_graph.txt -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/vinay/output/outdegree-3 -w 2

hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.InDegreeCountVertex -vif org.archive.giraph.LongDoubleFloatTextInputFormat -vip /user/vinay/tab_graph.txt -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/vinay/output/indegree-3 -w 2


#hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.PageRankVertex -vif org.archive.giraph.LongDoubleFloatTextInputFormat -vip /user/vinay/tab_graph.txt -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/vinay/output/pagerank4 -w 2 -mc org.archive.giraph.PageRankVertex\$PageRankVertexMasterCompute

hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.InDegreeCountVertex -vif org.archive.giraph.VertexWithDoubleValueLongDoubleFloatTextInputFormat -vip /user/vinay/pr-tab-id.graph.gz -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/vinay/output/indegree-4 -w 2



hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.InDegreeCountVertexWithTextValue -vif org.archive.giraph.VertexWithTextValueLongTextTextTextInputFormat -vip /user/vinay/pr-tab-id.graph-3 -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/vinay/output/indegree-36 -w 2


hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.InDegreeCountVertexWithTextValue -vif org.archive.giraph.VertexWithTextValueLongTextTextTextInputFormat -vip /search/nara/congress112th/giraph/crawl.id.hoppath/part* -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /search/nara/congress112th/giraph/crawl.id.indegree -w 50


hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.LabelPath -vif org.archive.giraph.VertexWithTextValueLongTextTextTextInputFormat -vip /search/nara/congress112th/giraph/crawl.id.hoppath/part* -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /search/nara/congress112th/giraph/crawl.id.hop-path-from-crawler-3/ -w 100



hadoop jar target/ia-giraph-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.archive.giraph.PageRank -vif org.archive.giraph.VertexWithDoubleValueLongDoubleFloatTextInputFormat -vip /search/nara/congress112th/pr-tab-id.graph/part* -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /search/nara/congress112th/pr-scores-giraph/ -w 100 -mc org.archive.giraph.PageRank\$PageRankMasterCompute
