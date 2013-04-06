package com.wtfleming.hadoop;

import  org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("post-comment-building", PostCommentBuilding.class, 
          "A map/reduce program that groups together posts and associated comments.");
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
