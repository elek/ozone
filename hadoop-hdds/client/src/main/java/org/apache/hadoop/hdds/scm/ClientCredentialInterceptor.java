package org.apache.hadoop.hdds.scm;

import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;

import static org.apache.hadoop.ozone.OzoneConsts.OBT_METADATA_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.USER_METADATA_KEY;

/**
 * GRPC client interceptor for ozone block token.
 */
public class ClientCredentialInterceptor implements ClientInterceptor {

  private final String user;
  private final String token;

  public ClientCredentialInterceptor(String user, String token) {
    this.user = user;
    this.token = token;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions,
      Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        if (token != null) {
          headers.put(OBT_METADATA_KEY, token);
        }
        if (user != null) {
          headers.put(USER_METADATA_KEY, user);
        }
        super.start(responseListener, headers);
      }
    };
  }
}
