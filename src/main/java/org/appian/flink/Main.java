package org.appian.flink;

public class Main {
  public static void main(String[] args) {
    if (args.length == 0) {
      args = new String[] {"keyBy"};
    }
    String processMethod = args[0];
    System.out.println("Processing the Job : " + args[0]);
    try {
      if ("filter".equalsIgnoreCase(processMethod)) {
        new FlinkJobFilter().start();
      } else if ("sideoutput".equalsIgnoreCase(processMethod)) {
        new FlinkJobSideOutput().start();
      } else {
        new FlinkJob().start();
      }
    } catch (Exception e) {
      System.out.println("Exception occurred");
      throw new RuntimeException(e);
    }
  }

}
