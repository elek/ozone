package org.apache.hadoop.ozone.container.replication;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.stream.StreamingDestination;

public class ContainerStreamingDestination implements StreamingDestination {

  private final KeyValueContainerData containerData;

  public ContainerStreamingDestination(KeyValueContainerData containerData) {
    this.containerData = containerData;
  }

  @Override
  public Path mapToDestination(String name) {
    String[] parts = name.split("/", 2);
    if (parts[0].equals("DB")) {
      return Paths.get(containerData.getContainerDBFile().getAbsolutePath()
          , parts[1]);
    }
    throw new IllegalArgumentException("Unknown container part:" + parts[0]);
  }
}
