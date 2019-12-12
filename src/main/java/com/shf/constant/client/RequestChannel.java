package com.shf.constant.client;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
        Mono<RSocket> socket = RSocketFactory.connect()
                .setupPayload(DefaultPayload.create("clientId_004", "connect-meta"))
                .transport(TcpClientTransport.create(HOST, PORT))
                .start();

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
