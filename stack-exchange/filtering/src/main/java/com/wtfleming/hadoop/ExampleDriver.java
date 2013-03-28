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
      
      
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
