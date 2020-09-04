package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;

public interface ContainerMetadataProvider {

  ContainerMetadataLease getMetadata(ContainerData containerData)
      throws StorageContainerException;

}
