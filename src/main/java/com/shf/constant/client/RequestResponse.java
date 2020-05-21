package com.shf.constant.client;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import static com.shf.constant.Constants.HOST;
import static com.shf.constant.Constants.PORT;

/**
 * Description:
 * Request/response  interaction model.
 *
 * @author: songhaifeng
 * @date: 2019/12/11 01:30
 */
@Slf4j
public class RequestResponse {

    public static void main(String[] args) {
        Mono<RSocket> socket =  RSocketConnector.create()
                .setupPayload(DefaultPayload.create("clientId_002","connect-meta"))
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .connect(TcpClientTransport.create(HOST, PORT));

        socket.blockOptional()
                .ifPresent(rSocket -> {
                    rSocket.requestResponse(DefaultPayload.create("shf","requester-meta"))
                            .doOnNext(payload -> log.info("Received response payload:[{}] metadata:[{}]",
                                    payload.getDataUtf8(),
                                    payload.getMetadataUtf8()))
                            .block();
                    rSocket.dispose();
                });
    }

}
