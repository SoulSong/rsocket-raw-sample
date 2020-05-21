package com.shf.constant.client;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.shf.constant.Constants.CLIENT_TAG;
import static com.shf.constant.Constants.HOST;
import static com.shf.constant.Constants.PORT;

/**
 * Description:
 * Fire-and-forget interaction model.
 * Additional feature :
 * - resume test
 *
 * @author: songhaifeng
 * @date: 2019/12/11 01:12
 */
@Slf4j
public class FireAndForget {

    public static void main(String[] args) throws InterruptedException {
        /**
         * {@link RSocketConnector)
         */
        Payload setupPayload = DefaultPayload.create("clientId_001", "connect-meta");

        Supplier<ByteBuf> resumeTokenSupplier = ResumeFrameCodec::generateResumeToken;
        Duration resumeSessionDuration = Duration.ofMinutes(2);
        Duration resumeStreamTimeout = Duration.ofSeconds(10);
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
                token -> new InMemoryResumableFramesStore(CLIENT_TAG, 100_000);


        Mono<RSocket> socket = RSocketConnector.create()
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .setupPayload(setupPayload)
                .resume(new Resume()
                        .token(resumeTokenSupplier)
                        .storeFactory(resumeStoreFactory)
                        .streamTimeout(resumeStreamTimeout)
                        .sessionDuration(resumeSessionDuration)
                        .retry(Retry.fixedDelay(3, Duration.ofSeconds(1)).doBeforeRetry(signal -> log.warn("Resuming @ " + Instant.now())))
                        .cleanupStoreOnKeepAlive())
                .connect(TcpClientTransport.create(HOST, PORT));

        socket.blockOptional()
                .ifPresent(rSocket -> {
                    Payload payload = DefaultPayload.create("I am shf.", "requester-meta");
                    rSocket.fireAndForget(payload)
                            .doOnSuccess(c -> log.info("Send successfully."))
                            .block();

                });

        Thread.currentThread().join();
    }

}
