package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

import org.apache.hadoop.hdds.scm.XCeiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class MockXCeiverManager
    implements XCeiverClientFactory {

  @Override
  public Function<ByteBuffer, ByteString> byteBufferToByteStringConversion() {
    return byteBuffer -> ByteString.copyFrom(byteBuffer);
  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException {
    return new MockXCeiverClientSpi(pipeline);
  }

  @Override
  public void releaseClient(
      XceiverClientSpi xceiverClient, boolean invalidateClient
  ) {

  }

  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
      throws IOException {
    return new MockXCeiverClientSpi(pipeline);
  }

  @Override
  public void releaseClientForReadData(
      XceiverClientSpi xceiverClient,
      boolean b
  ) {

  }
}
