/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.utils;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Strings;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getSingleSCMAddress;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.sanitizeUserArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hdds stateless helper functions for server side components.
 */
public final class HddsServerUtil {

  private HddsServerUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsServerUtil.class);

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to the SCM.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM service endpoint.
   */
  public static InetSocketAddress getScmAddressForDataNodes(
      ConfigurationSource conf) {
    // We try the following settings in decreasing priority to retrieve the
    // target host.
    // - OZONE_SCM_DATANODE_ADDRESS_KEY
    // - OZONE_SCM_CLIENT_ADDRESS_KEY
    // - OZONE_SCM_NAMES
    //
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM client endpoint.
   */
  public static InetSocketAddress getScmClientBindAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT);

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM Block service.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   */
  public static InetSocketAddress getScmBlockClientBindAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT);

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by scm security server to
   * service clients.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM security service.
   */
  public static InetSocketAddress getScmSecurityInetAddress(
      ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host
            + ":" + port
            .orElse(conf.getInt(ScmConfigKeys
                    .OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
                ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT)));
  }

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to the SCM.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM service endpoint.
   */
  public static InetSocketAddress getScmDataNodeBindAddress(
      ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));
  }


  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to Recon.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM service endpoint.
   */
  public static InetSocketAddress getReconDataNodeBindAddress(
      ConfigurationSource conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_KEY);

    final OptionalInt port = getPortNumberFromConfigKeys(conf,
        ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(
            ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ReconConfigKeys.OZONE_RECON_DATANODE_PORT_DEFAULT));
  }

  /**
   * Returns the interval in which the heartbeat processor thread runs.
   *
   * @param conf - Configuration
   * @return long in Milliseconds.
   */
  public static long getScmheartbeatCheckerInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the heartbeat frequency from a datanode to
   * SCM.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getScmHeartbeatInterval(ConfigurationSource conf) {
    return conf.getTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        HDDS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the Stale Node interval, which is used by SCM to flag a datanode as
   * stale, if the heartbeat from that node has been missing for this duration.
   *
   * @param conf - Configuration.
   * @return - Long, Milliseconds to wait before flagging a node as stale.
   */
  public static long getStaleNodeInterval(ConfigurationSource conf) {

    long staleNodeIntervalMs =
        conf.getTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
            OZONE_SCM_STALENODE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    long heartbeatThreadFrequencyMs = getScmheartbeatCheckerInterval(conf);

    long heartbeatIntervalMs = getScmHeartbeatInterval(conf);


    // Make sure that StaleNodeInterval is configured way above the frequency
    // at which we run the heartbeat thread.
    //
    // Here we check that staleNodeInterval is at least five times more than the
    // frequency at which the accounting thread is going to run.
    staleNodeIntervalMs = sanitizeUserArgs(OZONE_SCM_STALENODE_INTERVAL,
        staleNodeIntervalMs, OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        heartbeatThreadFrequencyMs, 5, 1000);

    // Make sure that stale node value is greater than configured value that
    // datanodes are going to send HBs.
    staleNodeIntervalMs = sanitizeUserArgs(OZONE_SCM_STALENODE_INTERVAL,
        staleNodeIntervalMs, HDDS_HEARTBEAT_INTERVAL, heartbeatIntervalMs, 3,
        1000);
    return staleNodeIntervalMs;
  }

  /**
   * Gets the interval for dead node flagging. This has to be a value that is
   * greater than stale node value,  and by transitive relation we also know
   * that this value is greater than heartbeat interval and heartbeatProcess
   * Interval.
   *
   * @param conf - Configuration.
   * @return - the interval for dead node flagging.
   */
  public static long getDeadNodeInterval(ConfigurationSource conf) {
    long staleNodeIntervalMs = getStaleNodeInterval(conf);
    long deadNodeIntervalMs = conf.getTimeDuration(OZONE_SCM_DEADNODE_INTERVAL,
        OZONE_SCM_DEADNODE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    // Make sure that dead nodes Ms is at least twice the time for staleNodes
    // with a max of 1000 times the staleNodes.
    return sanitizeUserArgs(OZONE_SCM_DEADNODE_INTERVAL, deadNodeIntervalMs,
        OZONE_SCM_STALENODE_INTERVAL, staleNodeIntervalMs, 2, 1000);
  }

  /**
   * Timeout value for the RPC from Datanode to SCM, primarily used for
   * Heartbeats and container reports.
   *
   * @param conf - Ozone Config
   * @return - Rpc timeout in Milliseconds.
   */
  public static long getScmRpcTimeOutInMilliseconds(ConfigurationSource conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT,
        OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Log Warn interval.
   *
   * @param conf - Ozone Config
   * @return - Log warn interval.
   */
  public static int getLogWarnInterval(ConfigurationSource conf) {
    return conf.getInt(OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT,
        OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT);
  }

  /**
   * returns the Container port.
   * @param conf - Conf
   * @return port number.
   */
  public static int getContainerPort(ConfigurationSource conf) {
    return conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
  }

  public static String getOzoneDatanodeRatisDirectory(
      ConfigurationSource conf) {
    String storageDir = conf.get(
            OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }



  /**
   * Get the path for datanode id file.
   *
   * @param conf - Configuration
   * @return the path of datanode id as string
   */
  public static String getDatanodeIdFilePath(ConfigurationSource conf) {
    String dataNodeIDDirPath =
        conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR);
    if (dataNodeIDDirPath == null) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      if (metaDirPath == null) {
        // this means meta data is not found, in theory should not happen at
        // this point because should've failed earlier.
        throw new IllegalArgumentException("Unable to locate meta data" +
            "directory when getting datanode id path");
      }
      dataNodeIDDirPath = metaDirPath.toString();
    }
    // Use default datanode id file name for file path
    return new File(dataNodeIDDirPath,
        OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString();
  }

  /**
   * Create a scm security client.
   * @param conf    - Ozone configuration.
   *
   * @return {@link SCMSecurityProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocolClientSideTranslatorPB getScmSecurityClient(
      ConfigurationSource conf) throws IOException {

    Configuration hadoopConfiguration =
        HddsServerUtil.getLegacyHadoopConfiguration(conf);
    RPC.setProtocolEngine(hadoopConfiguration, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress address =
        getScmAddressForSecurityProtocol(conf);
    RetryPolicy retryPolicy =
        RetryPolicies.retryForeverWithFixedSleep(
            1000, TimeUnit.MILLISECONDS);
    return new SCMSecurityProtocolClientSideTranslatorPB(
        RPC.getProtocolProxy(SCMSecurityProtocolPB.class, scmVersion,
            address, UserGroupInformation.getCurrentUser(),
            hadoopConfiguration,
            NetUtils.getDefaultSocketFactory(hadoopConfiguration),
            Client.getRpcTimeout(hadoopConfiguration), retryPolicy).getProxy());
  }


  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for
   * {@link org.apache.hadoop.hdds.protocol.SCMSecurityProtocol}. If
   * {@link ScmConfigKeys#OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined or invalid
   */
  public static InetSocketAddress getScmAddressForSecurityProtocol(
      ConfigurationSource conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }
  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocol getScmSecurityClient(
      OzoneConfiguration conf, UserGroupInformation ugi) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmSecurityProtoAdd =
        getScmAddressForSecurityProtocol(conf);
    return new SCMSecurityProtocolClientSideTranslatorPB(
        RPC.getProxy(SCMSecurityProtocolPB.class, scmVersion,
            scmSecurityProtoAdd, ugi, conf,
            NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)));
  }

  /**
   * Initialize hadoop metrics system for Ozone servers.
   * @param configuration OzoneConfiguration to use.
   * @param serverName    The logical name of the server components.
   */
  public static MetricsSystem initializeMetrics(
      ConfigurationSource configuration, String serverName) {
    MetricsSystem metricsSystem = DefaultMetricsSystem.initialize(serverName);
    try {
      JvmMetrics.create(serverName,
          configuration.get(DFSConfigKeysLegacy.DFS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());
    } catch (MetricsException e) {
      LOG.info("Metrics source JvmMetrics already added to DataNode.");
    }
    return metricsSystem;
  }

  /**
   * Register the provided MBean with additional JMX ObjectName properties.
   * If additional properties are not supported then fallback to registering
   * without properties.
   *
   * @param serviceName   - see {@link MBeans#register}
   * @param mBeanName     - see {@link MBeans#register}
   * @param jmxProperties - additional JMX ObjectName properties.
   * @param mBean         - the MBean to register.
   * @return the named used to register the MBean.
   */
  public static ObjectName registerWithJmxProperties(
      String serviceName, String mBeanName, Map<String, String> jmxProperties,
      Object mBean) {
    try {

      // Check support for registering with additional properties.
      final Method registerMethod = MBeans.class.getMethod(
          "register", String.class, String.class,
          Map.class, Object.class);

      return (ObjectName) registerMethod.invoke(
          null, serviceName, mBeanName, jmxProperties, mBean);

    } catch (NoSuchMethodException | IllegalAccessException |
        InvocationTargetException e) {

      // Fallback
      if (LOG.isTraceEnabled()) {
        LOG.trace("Registering MBean {} without additional properties {}",
            mBeanName, jmxProperties);
      }
      return MBeans.register(serviceName, mBeanName, mBean);
    }
  }

  /**
   * Returns the hostname for this datanode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param conf Configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the dfs.datanode.dns.interface
   *                              option is used and the hostname can not be
   *                              determined
   */
  public static String getHostName(ConfigurationSource conf)
      throws UnknownHostException {
    String name = conf.get(DFSConfigKeysLegacy.DFS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      String dnsInterface = conf.get(
          DFSConfigKeysLegacy.HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(
          DFSConfigKeysLegacy.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
      boolean fallbackToHosts = false;

      if (dnsInterface == null) {
        // Try the legacy configuration keys.
        dnsInterface =
            conf.get(DFSConfigKeysLegacy.DFS_DATANODE_DNS_INTERFACE_KEY);
        nameServer =
            conf.get(DFSConfigKeysLegacy.DFS_DATANODE_DNS_NAMESERVER_KEY);
      } else {
        // If HADOOP_SECURITY_DNS_* is set then also attempt hosts file
        // resolution if DNS fails. We will not use hosts file resolution
        // by default to avoid breaking existing clusters.
        fallbackToHosts = true;
      }

      name = DNS.getDefaultHost(dnsInterface, nameServer, fallbackToHosts);
    }
    return name;
  }

  public static AuthenticationMethod getAuthenticationMethod(
      ConfigurationSource conf) {
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class,
          StringUtils.toUpperCase(value));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  public static Configuration getLegacyHadoopConfiguration(
      ConfigurationSource source) {
    if (source instanceof Configuration) {
      return (Configuration) source;
    } else {
      throw new IllegalArgumentException(
          "ConffigurationSource should extends Hadoop configuration to be "
              + "used as a Hadoop configuration");
    }
  }
}
