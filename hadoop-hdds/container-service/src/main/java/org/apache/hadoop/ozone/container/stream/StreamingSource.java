package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Path;
import java.util.Map;

public interface StreamingSource {

  Map<String, Path> getFilesToStream(String id);

}
