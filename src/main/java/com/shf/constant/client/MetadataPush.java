package com.shf.constant.client;

import com.shf.constant.Constants;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Description:
 * Send metadata with Metadata-Push.
 *
 * @author: songhaifeng
 * @date: 2019/12/11 02:08
 */
@Slf4j
public class MetadataPush {

    public static void main(String[] args) throws InterruptedException {
        Mono<RSocket> socket = RSocketFactory.connect()
                .setupPayload(DefaultPayload.create("clientId_005", "connect-meta"))
                .transport(TcpClientTransport.create(Constants.HOST, Constants.PORT))
                .start();

        socket.blockOptional()
                .ifPresent(rSocket -> {
                    // Data will be removed when sent by `metadataPush`
                    rSocket.metadataPush(DefaultPayload.create("shf", "requester-meta"))
                            .block();
                });
        Thread.currentThread().join();
    }
}
