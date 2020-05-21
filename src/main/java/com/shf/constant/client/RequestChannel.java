package com.shf.constant.client;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static com.shf.constant.Constants.HOST;
import static com.shf.constant.Constants.PORT;

/**
 * Description:
 * Channel(bi-directional streams) interaction model.
 *
 * @author: songhaifeng
 * @date: 2019/12/11 02:03
 */
@Slf4j
public class RequestChannel {

    public static void main(String[] args) {
        Mono<RSocket> socket =  RSocketConnector.create()
                .setupPayload(DefaultPayload.create("clientId_004", "connect-meta"))
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .connect(TcpClientTransport.create(HOST, PORT));

        socket.blockOptional()
                .ifPresent(rSocket -> {
                    rSocket.requestChannel(Flux.interval(Duration.ofMillis(100))
                            .map(time -> DefaultPayload.create("shf")))
                            .doOnNext(payload -> log.info("Received response payload:[{}]",
                                    payload.getDataUtf8()))
                            .blockLast();
                });
    }


}
