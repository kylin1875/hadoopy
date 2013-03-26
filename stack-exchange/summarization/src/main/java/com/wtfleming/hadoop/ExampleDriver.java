package com.wtfleming.hadoop;

import  org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("wordcount", WordCount.class, 
                   "A map/reduce program that counts the words in the input files.");
      pgd.addClass("commentwordcount", CommentWordCount.class, 
                   "A map/reduce program that counts the number of words in the comments file.");
      pgd.addClass("minmaxcount", MinMaxCount.class,
                    "Get first, most recent, and total number of comments");
      pgd.addClass("average", Average.class,
              "Calculate average comment length by hour of day");
      pgd.addClass("medianstddev", MedianStandardDeviation.class,
              "Calculate median and standard deviation of comment length by hour of day");
      pgd.addClass("bettermedianstddev", BetterMedianStandardDeviation.class,
              "Calculate median and standard deviation of comment length by hour of day");
      pgd.addClass("invertedindexwikipedia", InvertedIndexWikipedia.class,
              "Generate an inverted index of links to wikipedia from posts");
      pgd.driver(argv);
      
      exitCode = 0;  // Success
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
