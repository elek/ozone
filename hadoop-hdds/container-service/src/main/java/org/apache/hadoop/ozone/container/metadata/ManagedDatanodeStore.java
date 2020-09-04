package org.apache.hadoop.ozone.container.metadata;

public interface ManagedDatanodeStore extends DatanodeStore {
  void stop() throws Exception;
}
