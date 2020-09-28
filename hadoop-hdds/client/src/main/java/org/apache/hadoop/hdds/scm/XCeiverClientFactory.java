package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public interface XCeiverClientFactory {
  Function<ByteBuffer, ByteString> byteBufferToByteStringConversion();

  XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException;

  void releaseClient(XceiverClientSpi xceiverClient, boolean invalidateClient);

  XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
      throws IOException;

  void releaseClientForReadData(XceiverClientSpi xceiverClient, boolean b);
}
