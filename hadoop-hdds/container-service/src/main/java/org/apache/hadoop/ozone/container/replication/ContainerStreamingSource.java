package org.apache.hadoop.ozone.container.replication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.stream.StreamingSource;

public class ContainerStreamingSource implements StreamingSource {

  private ContainerSet containerSet;

  public ContainerStreamingSource(ContainerSet containerSet) {
    this.containerSet = containerSet;
  }

  @Override
  public Map<String, Path> getFilesToStream(String id) {

    Map<String, Path> filesToStream = new HashMap<>();

    final Long containerId = Long.valueOf(id);
    final KeyValueContainer container =
        (KeyValueContainer) containerSet.getContainer(containerId);
    if (container == null) {
      throw new IllegalArgumentException("No such container " + containerId);
    }
    final File dbPath =
        container.getContainerData().getContainerDBFile();
    try {
      final List<Path> dbFiles =
          Files.list(dbPath.toPath()).collect(Collectors.toList());
      for (Path dbFile : dbFiles) {
        filesToStream.put("DB/" + dbFile.getFileName().toString(), dbFile);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return filesToStream;
  }
}
