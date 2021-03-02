package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Paths;

import io.netty.channel.Channel;

public class SimpleStreamingClient {

  public static void main(String[] args) throws Exception {
    new SimpleStreamingClient().run(args[0], args[1]);
  }

  public void run(String root, String target) throws Exception {
    StreamingClient client =
        new StreamingClient("localhost", 1234,
            new DirectoryServerDestination(
                Paths.get(root)));
    final Channel connect = client.connect();
    connect.writeAndFlush(target + "\n").await();
    connect.closeFuture().sync().await();
    System.out.println("done");
    client.close();
  }

}
