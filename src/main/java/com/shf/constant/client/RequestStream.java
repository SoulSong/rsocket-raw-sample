package com.shf.constant.client;

import com.shf.constant.Constants;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Description:
 * Request/stream  interaction model.
 * Additional fetaure:
 * - back pressure test
 *
 * @author: songhaifeng
 * @date: 2019/12/11 02:01
 */
@Slf4j
public class RequestStream {
    private static final int REQUEST_LIMIT = 10;

    public static void main(String[] args) throws InterruptedException {

        Mono<RSocket> socket = RSocketFactory.connect()
                .setupPayload(DefaultPayload.create("clientId_003", "connect-meta"))
                .transport(TcpClientTransport.create(Constants.HOST, Constants.PORT))
                .start();

        socket.blockOptional()
                .ifPresent(rSocket -> {
                    rSocket.requestStream(DefaultPayload.create("shf", "requester-meta"))
                            .limitRequest(REQUEST_LIMIT)
                            .doOnNext(payload -> log.info("Received response payload:[{}] metadata:[{}]",
                                    payload.getDataUtf8(),
                                    payload.getMetadataUtf8()))
                            .doFinally(c -> rSocket.dispose())
                            .subscribe(new BackPressureSubscriber());
                });
        Thread.currentThread().join();
    }

    static class BackPressureSubscriber implements Subscriber<Payload> {

        private static final Integer NUMBER_OF_REQUESTS_TO_PROCESS = 3;
        private Subscription subscription;
        int receivedItems;

        @Override
        public void onSubscribe(Subscription s) {
            this.subscription = s;
            subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS);
        }

        @Override
        public void onNext(Payload payload) {
            receivedItems++;
            if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
                log.info("Requesting next [{}] elements", NUMBER_OF_REQUESTS_TO_PROCESS);
                subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS);
            }
        }

        @Override
        public void onError(Throwable t) {
            log.error("Stream subscription error [{}]", t);
        }

        @Override
        public void onComplete() {
            log.info("Completing subscription");
        }
    }


}
