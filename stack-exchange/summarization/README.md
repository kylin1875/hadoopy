hadoop jar target/hadoop-summarization-1.0-jar-with-dependencies.jar wordcount ../../datasets/mobydick.txt output/wordcount/

hadoop jar target/hadoop-summarization-1.0-jar-with-dependencies.jar commentwordcount ../../datasets/gaming-stack-exchange/comments.xml output/comments/

hadoop jar target/hadoop-summarization-1.0-jar-with-dependencies.jar minmaxcount ../../datasets/gaming-stack-exchange/comments.xml output/min-max-count/
