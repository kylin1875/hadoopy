package com.wtfleming.hadoop;

import  org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("distributed-grep", DistributedGrep.class, 
                   "A map/reduce program implementation of grep.");
      pgd.addClass("simple-random-sampling", SimpleRandomSampling.class, 
              	   "Randomly sample records");
      pgd.addClass("bloom-filtering", BloomFiltering.class, 
         	   "Bloom filter");
      pgd.addClass("top-ten", TopTen.class, 
        	   "Find top 10 users ranked by reputation");
      pgd.addClass("distinct-users", DistinctUsers.class, 
       	   "Get a list of distinct user ids");
      pgd.addClass("unique-user-count", UniqueUserCount.class, 
          	   "Get a count of the number of unique users");
      
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
