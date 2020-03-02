/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_PORT_DEFAULT;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDDS specific stateless utility functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class HddsUtils {

  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  /**
   * number of nano seconds in 1 millisecond.
   */
  private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

  private static final Logger LOG = LoggerFactory.getLogger(HddsUtils.class);

  /**
   * The service ID of the solitary Ozone SCM service.
   */
  public static final String OZONE_SCM_SERVICE_ID = "OzoneScmService";
  public static final String OZONE_SCM_SERVICE_INSTANCE_ID =
      "OzoneScmServiceInstance";
  private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

  private static final String MULTIPLE_SCM_NOT_YET_SUPPORTED =
      ScmConfigKeys.OZONE_SCM_NAMES + " must contain a single hostname."
          + " Multiple SCM hosts are currently unsupported";

  private static final int NO_PORT = -1;

  private HddsUtils() {
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @return Target {@code InetSocketAddress} for the SCM client endpoint.
   */
  public static InetSocketAddress getScmAddressForClients(
      ConfigurationSource conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for block service. If
   * {@link ScmConfigKeys#OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
   *
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined.
   */
  public static InetSocketAddress getScmAddressForBlockClients(
      ConfigurationSource conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }



  /**
   * Retrieve the hostname, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf  - Conf
   * @param keys a list of configuration key names.
   *
   * @return first hostname component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static Optional<String> getHostNameFromConfigKeys(
      ConfigurationSource conf,
      String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final Optional<String> hostName = getHostName(value);
      if (hostName.isPresent()) {
        return hostName;
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the hostname or Indicates that it is absent.
   * @param value host or host:port
   * @return hostname
   */
  public static Optional<String> getHostName(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.empty();
    }
    String hostname = value.replaceAll("\\:[0-9]+$", "");
    if (hostname.length() == 0) {
      return Optional.empty();
    } else {
      return Optional.of(hostname);
    }
  }

  /**
   * Gets the port if there is one, returns empty {@code OptionalInt} otherwise.
   * @param value  String in host:port format.
   * @return Port
   */
  public static OptionalInt getHostPort(String value) {
    if ((value == null) || value.isEmpty()) {
      return OptionalInt.empty();
    }
    int port = HostAndPort.fromString(value).getPortOrDefault(NO_PORT);
    if (port == NO_PORT) {
      return OptionalInt.empty();
    } else {
      return OptionalInt.of(port);
    }
  }

  /**
   * Retrieve the port number, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf Conf
   * @param keys a list of configuration key names.
   *
   * @return first port number component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static OptionalInt getPortNumberFromConfigKeys(
      ConfigurationSource conf, String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final OptionalInt hostPort = getHostPort(value);
      if (hostPort.isPresent()) {
        return hostPort;
      }
    }
    return OptionalInt.empty();
  }

  /**
   * Retrieve the socket addresses of all storage container managers.
   *
   * @return A collection of SCM addresses
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static Collection<InetSocketAddress> getSCMAddresses(
      ConfigurationSource conf) {
    String[] names =
        conf.getTrimmedStrings(ScmConfigKeys.OZONE_SCM_NAMES);
    if (names.length == 0) {
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_NAMES
          + " need to be a set of valid DNS names or IP addresses."
          + " Empty address list found.");
    }

    Collection<InetSocketAddress> addresses = new HashSet<>(names.length);
    for (String address : names) {
      Optional<String> hostname = getHostName(address);
      if (!hostname.isPresent()) {
        throw new IllegalArgumentException("Invalid hostname for SCM: "
            + address);
      }
      int port = getHostPort(address)
          .orElse(ScmConfigKeys.OZONE_SCM_DEFAULT_PORT);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(), port);
      addresses.add(addr);
    }
    return addresses;
  }

  /**
   * Retrieve the socket addresses of recon.
   *
   * @return Recon address
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static InetSocketAddress getReconAddresses(
      ConfigurationSource conf) {
    String name = conf.get(OZONE_RECON_ADDRESS_KEY);
    if (name == null || name.length() == 0) {
      return null;
    }
    Optional<String> hostname = getHostName(name);
    if (!hostname.isPresent()) {
      throw new IllegalArgumentException("Invalid hostname for Recon: "
          + name);
    }
    int port = getHostPort(name).orElse(OZONE_RECON_DATANODE_PORT_DEFAULT);
    return NetUtils.createSocketAddr(hostname.get(), port);
  }

  /**
   * Retrieve the address of the only SCM (as currently multiple ones are not
   * supported).
   *
   * @return SCM address
   * @throws IllegalArgumentException if {@code conf} has more than one SCM
   *         address or it has none
   */
  public static InetSocketAddress getSingleSCMAddress(
      ConfigurationSource conf) {
    Collection<InetSocketAddress> singleton = getSCMAddresses(conf);
    Preconditions.checkArgument(singleton.size() == 1,
        MULTIPLE_SCM_NOT_YET_SUPPORTED);
    return singleton.iterator().next();
  }

  /**
   * Checks if the container command is read only or not.
   * @param proto ContainerCommand Request proto
   * @return True if its readOnly , false otherwise.
   */
  public static boolean isReadOnly(
      ContainerProtos.ContainerCommandRequestProto proto) {
    switch (proto.getCmdType()) {
    case ReadContainer:
    case ReadChunk:
    case ListBlock:
    case GetBlock:
    case GetSmallFile:
    case ListContainer:
    case ListChunk:
    case GetCommittedBlockLength:
      return true;
    case CloseContainer:
    case WriteChunk:
    case UpdateContainer:
    case CompactChunk:
    case CreateContainer:
    case DeleteChunk:
    case DeleteContainer:
    case DeleteBlock:
    case PutBlock:
    case PutSmallFile:
    default:
      return false;
    }
  }

  /**
   * Not all datanode container cmd protocol has embedded ozone block token.
   * Block token are issued by Ozone Manager and return to Ozone client to
   * read/write data on datanode via input/output stream.
   * Ozone datanode uses this helper to decide which command requires block
   * token.
   * @return true if it is a cmd that block token should be checked when
   * security is enabled
   * false if block token does not apply to the command.
   *
   */
  public static boolean requireBlockToken(
      ContainerProtos.Type cmdType) {
    switch (cmdType) {
    case ReadChunk:
    case GetBlock:
    case WriteChunk:
    case PutBlock:
    case PutSmallFile:
    case GetSmallFile:
      return true;
    default:
      return false;
    }
  }

  /**
   * Return the block ID of container commands that are related to blocks.
   * @param msg container command
   * @return block ID.
   */
  public static BlockID getBlockID(ContainerCommandRequestProto msg) {
    switch (msg.getCmdType()) {
    case ReadChunk:
      if (msg.hasReadChunk()) {
        return BlockID.getFromProtobuf(msg.getReadChunk().getBlockID());
      }
      return null;
    case GetBlock:
      if (msg.hasGetBlock()) {
        return BlockID.getFromProtobuf(msg.getGetBlock().getBlockID());
      }
      return null;
    case WriteChunk:
      if (msg.hasWriteChunk()) {
        return BlockID.getFromProtobuf(msg.getWriteChunk().getBlockID());
      }
      return null;
    case PutBlock:
      if (msg.hasPutBlock()) {
        return BlockID.getFromProtobuf(msg.getPutBlock().getBlockData()
            .getBlockID());
      }
      return null;
    case PutSmallFile:
      if (msg.hasPutSmallFile()) {
        return BlockID.getFromProtobuf(msg.getPutSmallFile().getBlock()
            .getBlockData().getBlockID());
      }
      return null;
    case GetSmallFile:
      if (msg.hasGetSmallFile()) {
        return BlockID.getFromProtobuf(msg.getGetSmallFile().getBlock()
            .getBlockID());
      }
      return null;
    default:
      return null;
    }
  }

  /**
   * Get the current UTC time in milliseconds.
   * @return the current UTC time in milliseconds.
   */
  public static long getUtcTime() {
    return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
  }

  /**
   * Basic validation for {@code path}: checks that it is a descendant of
   * (or the same as) the given {@code ancestor}.
   * @param path the path to be validated
   * @param ancestor a trusted path that is supposed to be the ancestor of
   *     {@code path}
   * @throws NullPointerException if either {@code path} or {@code ancestor} is
   *     null
   * @throws IllegalArgumentException if {@code ancestor} is not really the
   *     ancestor of {@code path}
   */
  public static void validatePath(Path path, Path ancestor) {
    Preconditions.checkNotNull(path,
        "Path should not be null");
    Preconditions.checkNotNull(ancestor,
        "Ancestor should not be null");
    Preconditions.checkArgument(
        path.normalize().startsWith(ancestor.normalize()),
        "Path should be a descendant of %s", ancestor);
  }

  public static String writeChunkToString(WriteChunkRequestProto wc,
                                          long contId, String location) {
    Preconditions.checkNotNull(wc);
    StringBuilder builder = new StringBuilder();

    builder.append("cmd=");
    builder.append(ContainerProtos.Type.WriteChunk.toString());

    builder.append(", container id=");
    builder.append(contId);

    builder.append(", blockid=");
    builder.append(wc.getBlockID().getContainerID());
    builder.append(":localid=");
    builder.append(wc.getBlockID().getLocalID());

    builder.append(", chunk=");
    builder.append(wc.getChunkData().getChunkName());
    builder.append(":offset=");
    builder.append(wc.getChunkData().getOffset());
    builder.append(":length=");
    builder.append(wc.getChunkData().getLen());

    builder.append(", container path=");
    builder.append(location);

    return builder.toString();
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   * @param conf Configuration instance
   * @param alias name of the credential to retreive
   * @return String credential value or null
   */
  static String getPassword(ConfigurationSource conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (Exception ioe) {
      LOG.warn("Setting password to null since IOException is caught"
          + " when getting password", ioe);

      password = null;
    }
    return password;
  }

  /**
   * Current time from some arbitrary time base in the past, counting in
   * milliseconds, and not affected by settimeofday or similar system clock
   * changes.  This is appropriate to use when computing how much longer to
   * wait for an interval to expire.
   * This function can return a negative value and it must be handled correctly
   * by callers. See the documentation of System#nanoTime for caveats.
   *
   * @return a monotonic clock that counts in milliseconds.
   */
  public static long monotonicNow() {
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }

  /**
   * Convenience method that returns a resource as inputstream from the
   * classpath using given classloader.
   * <p>
   *
   * @param cl           ClassLoader to be used to retrieve resource.
   * @param resourceName resource to retrieve.
   * @return inputstream with the resource.
   * @throws IOException thrown if resource cannot be loaded
   */
  public static InputStream getResourceAsStream(ClassLoader cl,
      String resourceName)
      throws IOException {
    if (cl == null) {
      throw new IOException("Can not read resource file '" + resourceName +
          "' because given class loader is null");
    }
    InputStream is = cl.getResourceAsStream(resourceName);
    if (is == null) {
      throw new IOException("Can not read resource file '" +
          resourceName + "'");
    }
    return is;
  }

  /**
   * Create an object for the given class.
   *
   * @param theClass class of which an object is created
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
