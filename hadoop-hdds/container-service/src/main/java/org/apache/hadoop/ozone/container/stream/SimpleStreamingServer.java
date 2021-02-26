package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Paths;

public class SimpleStreamingServer {

  public static void main(String[] args) throws Exception {
    new SimpleStreamingServer().run(args[0]);
  }

  public void run(String root) throws Exception {
    StreamingServer server = new StreamingServer(new DirectoryServerSource(
        Paths.get(root)), 1234);
    server.start();
    System.out.println(server.getPort());
    Thread.sleep(10000L);
  }
}
