hadoop jar target/hadoop-data-organization-1.0-jar-with-dependencies.jar post-comment-building ../../datasets/gaming-stack-exchange/Posts.xml ../../datasets/gaming-stack-exchange/Comments.xml output/post-comment-building

hadoop jar target/hadoop-data-organization-1.0-jar-with-dependencies.jar question-answer-hierarchy output/post-comment-building/part-r-00000 output/question-answer-building

hadoop jar target/hadoop-data-organization-1.0-jar-with-dependencies.jar partition-users ../../datasets/gaming-stack-exchange/Users.xml output/partition-users

hadoop jar target/hadoop-data-organization-1.0-jar-with-dependencies.jar binning ../../datasets/gaming-stack-exchange/Posts.xml output/binning


hadoop jar target/hadoop-data-organization-1.0-jar-with-dependencies.jar total-order-sorting ../../datasets/gaming-stack-exchange/Users.xml output/total-order-sorting 0.1


