package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;

import java.io.IOException;
import java.io.OutputStream;

public class ClosedContainerStreamGenerator implements ContainerReplicationSource {

  private ContentGenerator generator = new ContentGenerator(5L * 1024 * 1024 * 1024, 10 * 1024 * 1024);

  @Override
  public void prepare(long containerId) {

  }

  @Override
  public void copyData(long containerId, OutputStream destination) throws IOException {
    generator.write(destination);
    destination.close();
  }
}
