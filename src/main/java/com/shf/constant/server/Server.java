package com.shf.constant.server;

import io.netty.buffer.ByteBuf;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.exceptions.SetupException;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import static com.shf.constant.Constants.HOST;
import static com.shf.constant.Constants.PORT;
import static com.shf.constant.Constants.SERVER_TAG;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/12/11 00:38
 */
@Slf4j
public class Server {

    public static void main(String[] args) throws InterruptedException {
        final Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
                token -> new InMemoryResumableFramesStore(SERVER_TAG, 100_000);
        Duration resumeSessionDuration = Duration.ofSeconds(120);
        Duration resumeStreamTimeout = Duration.ofSeconds(10);

        RSocketServer.create()
                .acceptor(new MessageSocketAcceptor())
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .resume(new Resume()
                        .sessionDuration(resumeSessionDuration)
                        .streamTimeout(resumeStreamTimeout)
                        .storeFactory(resumeStoreFactory)
                        .retry(Retry.fixedDelay(10, Duration.ofSeconds(1))))
                .bind(TcpServerTransport.create(HOST, PORT))
                .doOnCancel(() -> {
                    log.warn("requester is canceled.");
                })
                .doOnError(error -> {
                    log.error("Server start error : {}", error.getMessage());
                    System.exit(1);
                })
                .doOnSuccess(closeableChannel -> log.info("Server is running"))
                .subscribe();

        Thread.currentThread().join();
    }

    @Slf4j
    static class MessageSocketAcceptor implements SocketAcceptor {

        /**
         * Handle the {@code SETUP} frame for a new connection and create a responder {@code RSocket} for
         * handling requests from the remote peer.
         *
         * @param setup         the {@code setup} received from a client in a server scenario, or in a client
         *                      scenario this is the setup about to be sent to the server.
         * @param sendingSocket socket for sending requests to the remote peer.
         * @return {@code RSocket} to accept requests with.
         * @throws SetupException If the acceptor needs to reject the setup of this socket.
         */
        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            log.info("Received connection with setup payload: [{}] and meta-data: [{}]", setup.getDataUtf8(), setup.getMetadataUtf8());

            return Mono.just(new RSocket() {
                @Override
                public Mono<Void> fireAndForget(Payload payload) {
                    log.info("Received 'fire-and-forget' request with payload: [{}] and meta-data: [{}]", payload.getDataUtf8(), payload.getMetadataUtf8());
                    return Mono.empty();
                }

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    log.info("Received 'request/response' request with payload: [{}] and meta-data: [{}]", payload.getDataUtf8(), payload.getMetadataUtf8());
                    return Mono.just(DefaultPayload.create("Hello " + payload.getDataUtf8(), "responder-meta"));
                }

                @Override
                public Flux<Payload> requestStream(Payload payload) {
                    log.info("Received 'request/stream' request with payload: [{}] and meta-data: [{}]", payload.getDataUtf8(), payload.getMetadataUtf8());
                    return Flux.interval(Duration.ofSeconds(1))
                            .map(time -> DefaultPayload.create("Hello " + payload.getDataUtf8() + " @ " + Instant.now(), "responder-meta"));
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return Flux.from(payloads)
                            .doOnNext(payload -> {
                                log.info("Received 'requestChannel' request with payload: [{}]", payload.getDataUtf8());
                            })
                            .doOnCancel(() -> log.info("Cancel"))
                            .map(payload -> DefaultPayload.create("Hello " + payload.getDataUtf8() + " @ " + Instant.now()))
                            .subscribeOn(Schedulers.parallel());
                }

                /**
                 * Only receive metadata
                 * @param payload payload
                 * @return void
                 */
                @Override
                public Mono<Void> metadataPush(Payload payload) {
                    log.info("Received 'metadata push' request with payload: [{}] and meta-data: [{}]", payload.getDataUtf8(), payload.getMetadataUtf8());
                    return Mono.empty();
                }
            });
        }
    }

}
