package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_READ_METADATA_DB;

public class DefaultContainerMetadataProvider
    implements ContainerMetadataProvider {

  ConfigurationSource conf;

  @Override
  public ContainerMetadataLease getMetadata(ContainerData containerMetadata)
      throws StorageContainerException {
    KeyValueContainerData containerData =
        (KeyValueContainerData) containerMetadata;
    Preconditions.checkNotNull(containerData);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    Preconditions.checkNotNull(containerData.getDbFile());
    try {
      return cache.getDB(containerData.getContainerID(), containerData
              .getContainerDBType(),
          containerData.getDbFile().getAbsolutePath(),
          containerData.getSchemaVersion(), conf);

    } catch (IOException ex) {
      String message = String.format("Error opening DB. Container:%s " +
          "ContainerPath:%s", containerData.getContainerID(), containerData
          .getDbFile().getPath());
      throw new StorageContainerException(message, UNABLE_TO_READ_METADATA_DB);
    }
  }
}
