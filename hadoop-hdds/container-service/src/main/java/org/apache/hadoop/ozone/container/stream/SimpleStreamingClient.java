package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Paths;

public class SimpleStreamingClient {

  public static void main(String[] args) throws Exception {
    new SimpleStreamingClient().run(args[0], args[1]);
  }

  public void run(String root, String target) throws Exception {
    StreamingClient client =
        new StreamingClient("localhost", 1234,
            new DirectoryServerDestination(
                Paths.get(root)));
    client.connect().writeAndFlush(target + "\n").await();

  }

}
