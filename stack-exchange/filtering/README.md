You can generate a bloom filter file in ../bloom-filter, or use the included hotlist.blm

hadoop jar target/hadoop-filtering-1.0-jar-with-dependencies.jar bloom-filtering ../../datasets/gaming-stack-exchange/comments.xml hotlist.blm output/bloom-filtering/

hadoop jar target/hadoop-filtering-1.0-jar-with-dependencies.jar top-ten ../../datasets/gaming-stack-exchange/users.xml output/top-ten/

hadoop jar target/hadoop-filtering-1.0-jar-with-dependencies.jar distinct-users ../../datasets/gaming-stack-exchange/comments.xml output/distinct-users/



Other jobs:
distributed-grep
simple-random-sampling

