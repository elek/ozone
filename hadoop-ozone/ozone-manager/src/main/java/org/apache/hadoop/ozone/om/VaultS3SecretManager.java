package org.apache.hadoop.ozone.om;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

import java.io.IOException;

@ConfigGroup(prefix = "ozone.s3.auth.vault")
public class VaultS3SecretManager implements S3SecretManager {

  @Config(key = "host",
      defaultValue = "http://127.0.0.1:8200",
      type = ConfigType.STRING,
      description = "Full http(s) URL to acccess Hashicorp vault",
      tags = ConfigTag.SECURITY)
  private String host;

  @Config(key = "token",
      defaultValue = "",
      type = ConfigType.STRING,
      description = "Token to access vault store",
      tags = ConfigTag.SECURITY)
  private String token;

  @Config(key = "prefix",
      defaultValue = "secret/s3/",
      type = ConfigType.STRING,
      description = "Vault prefix to find the secrets",
      tags = ConfigTag.SECURITY)
  private String prefix;

  private Vault vault;

  public VaultS3SecretManager() {
  }

  public void init() {
    try {
      final VaultConfig config =
          new VaultConfig()
              .address(host)
              .token(token)
              .build();
      vault = new Vault(config);
    } catch (VaultException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getS3UserSecretString(String awsAccessKey) throws IOException {
    try {
      return vault.logical()
          .read(prefix + awsAccessKey).getData()
          .get("key");
    } catch (VaultException e) {
      throw new IOException("Couldn't read vault secret", e);
    }
  }
}
