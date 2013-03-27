package com.wtfleming.hadoop;

import  org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("distributedgrep", DistributedGrep.class, 
                   "A map/reduce program implementation of grep.");
      pgd.addClass("simple-random-sampling", SimpleRandomSampling.class, 
              	   "Randomly sample records");
      
      
      
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
