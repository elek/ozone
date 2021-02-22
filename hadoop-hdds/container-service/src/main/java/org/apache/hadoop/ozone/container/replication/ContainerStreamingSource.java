package org.apache.hadoop.ozone.container.replication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    try {

      final File dbPath =
          container.getContainerData().getContainerDBFile();

      final List<Path> dbFiles =
          Files.list(dbPath.toPath()).collect(Collectors.toList());
      for (Path dbFile : dbFiles) {
        filesToStream.put("DB/" + dbFile.getFileName().toString(), dbFile);
      }

      filesToStream.put("container.yaml",
          container.getContainerData().getContainerFile().toPath());

      final String dataPath =
          container.getContainerData().getContainerPath();
      final List<Path> dataFiles =
          Files.list(Paths.get(dataPath)).collect(Collectors.toList());
      for (Path dataFile : dataFiles) {
        if (!Files.isDirectory(dataFile)) {
          filesToStream
              .put("DATA/" + dataFile.getFileName().toString(), dataFile);
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Couldn't stream countainer " + containerId,
          e);
    }
    return filesToStream;
  }
}
