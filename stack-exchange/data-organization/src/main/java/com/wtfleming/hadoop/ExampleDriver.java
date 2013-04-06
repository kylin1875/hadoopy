package com.wtfleming.hadoop;

import  org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("post-comment-building", PostCommentBuilding.class, 
          "A map/reduce program that groups together posts and associated comments.");
      pgd.addClass("question-answer-hierarchy", QuestionAnswerBuilding.class, 
          "A map/reduce program that groups together questions and their answers.");
      pgd.addClass("partition-users", PartitionUsers.class, 
          "A map/reduce program that partitions users based on the last time they logged on.");
      pgd.addClass("binning", Binning.class, 
          "A map/reduce program that bins posts based on their tags.");
      pgd.addClass("total-order-sorting", TotalOrderSorting.class, 
          "A map/reduce program that sorts users based on last time they logged on.");
      pgd.addClass("anonymize-users", AnonymizeUsers.class, 
          "A map/reduce program that sorts.");
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
