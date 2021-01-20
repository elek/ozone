package org.apache.hadoop.ozone.common;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public class JDK9Crc32CByteBuffer
    implements ChecksumByteBuffer {

  private CRC32C checksum = new CRC32C();

  @Override
  public void update(ByteBuffer buffer) {
    checksum.update(buffer);
  }

  @Override
  public void update(byte[] b, int off, int len) {
    checksum.update(b, off, len);
  }

  @Override
  public void update(int i) {
    checksum.update(i);
  }

  @Override
  public long getValue() {
    return checksum.getValue();
  }

  @Override
  public void reset() {
    checksum.reset();
  }
}
