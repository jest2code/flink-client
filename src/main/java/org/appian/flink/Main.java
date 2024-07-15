package org.appian.flink;

public class Main {
  public static void main(String[] args) {
    System.out.println("Hello world!");
    try {
      new FlinkJob().start();
    } catch (Exception e) {
      System.out.println("Exception occurred");
      throw new RuntimeException(e);
    }
  }



}
