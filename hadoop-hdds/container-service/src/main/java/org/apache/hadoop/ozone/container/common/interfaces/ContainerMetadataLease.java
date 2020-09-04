package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.Closeable;

import org.apache.hadoop.ozone.container.metadata.DatanodeStore;

public interface ContainerMetadataLease extends Closeable {

  public DatanodeStore getStore();

  public void close();
}
