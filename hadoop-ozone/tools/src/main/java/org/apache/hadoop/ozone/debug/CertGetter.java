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
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getX509Certificate;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;

/**
 * Tool to upgrade Datanode layout.
 */
@CommandLine.Command(
    name = "getcert",
    description = "Get certificate from SCM",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class CertGetter extends GenericCli
    implements Callable<Void>, SubcommandWithParent {

  private static final Logger LOG = LoggerFactory.getLogger(CertGetter.class);
  @Spec
  private CommandSpec spec;

  public static void main(String[] args) {
    new CertGetter().run(args);
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = createOzoneConfiguration();
    DatanodeDetails.Builder dnb = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .setNetworkLocation("/rack1")
        .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
        .setPersistedOpStateExpiry(0);

    for (DatanodeDetails.Port.Name name : ALL_PORTS) {
      dnb.addPort(DatanodeDetails.newPort(name, 1234));
    }

    for (DatanodeDetails.Port.Name name : ALL_PORTS) {
      dnb.addPort(DatanodeDetails.newPort(name, 1234));
    }
    final DatanodeDetails datanodeDetails = dnb.build();

    CertificateClient dnCertClient =
        new DNCertificateClient(
            new SecurityConfig(conf),
            datanodeDetails.getCertSerialId());
    dnCertClient.init();

    final PKCS10CertificationRequest csr = getCSR(dnCertClient, conf);
    getSCMSignedCert(datanodeDetails, conf, csr, dnCertClient);

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  private void getSCMSignedCert(DatanodeDetails datanodeDetails,
      OzoneConfiguration config,
      PKCS10CertificationRequest csr,
      CertificateClient dnCertClient) {
    try {
      // TODO: For SCM CA we should fetch certificate from multiple SCMs.
      SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
          HddsServerUtil.getScmSecurityClientWithMaxRetry(config);
      SCMSecurityProtocolProtos.SCMGetCertResponseProto response = secureScmClient.
          getDataNodeCertificateChain(
              datanodeDetails.getProtoBufMessage(),
              getEncodedString(csr));
      // Persist certificates.
      if (response.hasX509CACertificate()) {
        String pemEncodedCert = response.getX509Certificate();
        dnCertClient.storeCertificate(pemEncodedCert, true);
        dnCertClient.storeCertificate(response.getX509CACertificate(), true,
            true);

        // Store Root CA certificate.
        if (response.hasX509RootCACertificate()) {
          dnCertClient.storeRootCACertificate(
              response.getX509RootCACertificate(), true);
        }
        String dnCertSerialId = getX509Certificate(pemEncodedCert).
            getSerialNumber().toString();
        datanodeDetails.setCertSerialId(dnCertSerialId);
        // Rebuild dnCertClient with the new CSR result so that the default
        // certSerialId and the x509Certificate can be updated.
        dnCertClient = new DNCertificateClient(
            new SecurityConfig(config), dnCertSerialId);

      } else {
        throw new RuntimeException("Unable to retrieve datanode certificate " +
            "chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  public PKCS10CertificationRequest getCSR(CertificateClient dnCertClient, ConfigurationSource config)
      throws IOException {

    CertificateSignRequest.Builder builder = dnCertClient.getCSRBuilder();
    KeyPair keyPair = new KeyPair(dnCertClient.getPublicKey(),
        dnCertClient.getPrivateKey());

    String hostname = InetAddress.getLocalHost().getCanonicalHostName();
    String subject = UserGroupInformation.getCurrentUser()
        .getShortUserName() + "@" + hostname;

    builder.setCA(false)
        .setKey(keyPair)
        .setConfiguration(config)
        .setSubject(subject);

    LOG.info("Creating csr for DN-> subject:{}", subject);
    return builder.build();
  }

}