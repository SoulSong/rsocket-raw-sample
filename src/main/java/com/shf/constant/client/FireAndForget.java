package com.shf.constant.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.resume.ClientResume;
import io.rsocket.resume.ExponentialBackoffResumeStrategy;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import org.reactivestreams.Publisher;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import static com.shf.constant.Constants.CLIENT_TAG;
import static com.shf.constant.Constants.HOST;
import static com.shf.constant.Constants.PORT;

/**
 * Description:
 * Fire-and-forget interaction model
 *
 * @author: songhaifeng
 * @date: 2019/12/11 01:12
 */
@Slf4j
public class FireAndForget {

    public static void main(String[] args) throws InterruptedException {
        /**
         * {@link RSocketFactory.ClientRSocketFactory)
         */
        Payload setupPayload = DefaultPayload.create("clientId_001","connect-meta");
        Duration tickPeriod = Duration.ofSeconds(20);
        Duration ackTimeout = Duration.ofSeconds(30);
        int missedAcks = 2;

        String metadataMimeType = "application/binary";
        String dataMimeType = "application/binary";

        Supplier<ByteBuf> resumeTokenSupplier = ResumeFrameFlyweight::generateResumeToken;
        Duration resumeSessionDuration = Duration.ofMinutes(2);
        Duration resumeStreamTimeout = Duration.ofSeconds(10);
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
                token -> new InMemoryResumableFramesStore(CLIENT_TAG, 100_000);


        ByteBuf setupFrame =
                SetupFrameFlyweight.encode(
                        ByteBufAllocator.DEFAULT,
                        false,
                        (int) tickPeriod.toMillis(),
                        (int) (ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks),
                        resumeTokenSupplier.get(),
                        metadataMimeType,
                        dataMimeType,
                        setupPayload);

        Mono<RSocket> socket = RSocketFactory.connect()
                .setupPayload(ConnectionSetupPayload.create(setupFrame))
                .resume()
                .resumeStreamTimeout(resumeStreamTimeout)
                .resumeStore(resumeStoreFactory)
                .resumeSessionDuration(resumeSessionDuration)
                .resumeCleanupOnKeepAlive()
                .resumeStrategy(() -> new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2) {
                    @Override
                    public Publisher<?> apply(ClientResume clientResume, Throwable throwable) {
                        log.warn("Resuming @ " + Instant.now());
                        return super.apply(clientResume, throwable);
                    }
                })
                .transport(TcpClientTransport.create(HOST, PORT))
                .start();
        socket.blockOptional()
                .ifPresent(rSocket -> {
                    rSocket.fireAndForget(DefaultPayload.create("I am shf.", "requester-meta")).block();
                });
        Thread.currentThread().join();
    }

}
