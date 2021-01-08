package hu.fcs.testDrivenLearnin.reactor;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;


public class TestReactor {

    @Test
    public void testCreatePublisher() {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4);
        Flux<Integer> emptyFlux = Flux.empty();
        Mono<Integer> mono = Mono.just(1);
        Mono<Integer> emptyMono = Mono.empty();
    }

    @Test
    void testSubscribe() {
        // given
        List<Integer> elements = new ArrayList<>();

        // when
        Flux.just(1, 2, 3, 4)
            .log()
            .subscribe(elements::add);

        // then
        assertThat(elements).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testBackpressureWithSubscriberInterface() {
        // given
        List<Integer> elements = new ArrayList<>();

        //when
        Flux.just(1, 2, 3, 4)
            .log()
            .subscribe(new Subscriber<Integer>() {
                private Subscription s;
                int onNextAmount;

                @Override
                public void onSubscribe(Subscription s) {
                    this.s = s;
                    s.request(2);
                }

                @Override
                public void onNext(Integer integer) {
                    elements.add(integer);
                    onNextAmount++;
                    if (onNextAmount % 2 == 0) {
                        s.request(2);
                    }
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onComplete() {}
            });

        // then
        assertThat(elements).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testMap() {
        // given
        List<Integer> elements = new ArrayList<>();

        // when
        Flux.just(1, 2, 3, 4)
            .log()
            .map(i -> i * 2)
            .log()
            .subscribe(elements::add);

        // then
        assertThat(elements).containsExactly(2, 4, 6, 8);
    }

    @Test
    void testZip() {
        // given
        List<String> elements = new ArrayList<>();

        // when
        Flux.just(1, 2, 3, 4)
            .log()
            .map(i -> i * 2)
            .zipWith(Flux.range(0, Integer.MAX_VALUE).log(), 1,
                (one, two) -> String.format("First Flux: %d, Second Flux: %d", one, two))
            .subscribe(elements::add);

        // then
        assertThat(elements).containsExactly(
            "First Flux: 2, Second Flux: 0",
            "First Flux: 4, Second Flux: 1",
            "First Flux: 6, Second Flux: 2",
            "First Flux: 8, Second Flux: 3");
    }

    @Test
    void testHotStream() {
        // given
        List<Integer> elements = new ArrayList<>();
        ConnectableFlux<Integer> publish = Flux.just(1, 2, 3, 4).publish();
        publish.subscribe(elements::add);
        assertThat(elements).isEmpty();

        // when
        publish.connect();

        // then
        assertThat(elements).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testDelayElement() {
        // given
        List<Triplet<Long, Long, Long>> elements = new ArrayList<>();

        // when
        Flux.just(ts(), ts(), ts(), ts())
            .delayElements(Duration.ofSeconds(1))
            .log()
            .map(ts -> new Pair<Long, Long>(ts, ts()))
            .map(pair -> new Triplet<Long, Long, Long>(pair.getValue0(), pair.getValue1(), pair.getValue1() - pair.getValue0()))
            .log()
            .doOnNext(elements::add)
            .blockLast();

        // then
        long i = 1;
        for (Triplet<Long, Long, Long> triplet : elements) {
            assertThat(Math.abs(triplet.getValue2())).isCloseTo(1000L * i, offset(300L));
            i++;
        }

    }

    @Test
    void testDelaySubscription() {
        // given
        List<Triplet<Long, Long, Long>> elements = new ArrayList<>();

        // when
        Flux.just(ts(), ts(), ts(), ts())
            .log()
            .delaySubscription(Duration.ofSeconds(1))
            .log()
            .map(ts -> new Pair<Long, Long>(ts, ts()))
            .map(pair -> new Triplet<Long, Long, Long>(pair.getValue0(), pair.getValue1(), pair.getValue1() - pair.getValue0()))
            .log()
            .doOnNext(elements::add)
            .blockLast();

        // then
        for (Triplet<Long, Long, Long> triplet : elements) {
            assertThat(Math.abs(triplet.getValue2())).isCloseTo(1000L, offset(200L));
        }
    }

    private static long ts() {
        return System.currentTimeMillis();
    }

    @Test
    void testRetryMonoFromSupplierIsSuccessful() {
        // given
        LinkedList<Integer> queue = new LinkedList<>(asList(1, 2, 3, 4, 5, 6));
        Mono<Integer> mono = failSmallerThan(5, getMonoFromSupplier(queue));

        // when
        Integer result = mono.retry(4).block();

        // then
        assertThat(result).isEqualTo(5);
        assertThat(queue).hasSize(1);
    }

    @Test
    void testRetryMonoFromSupplierIsUnsuccessful() {
        // given
        LinkedList<Integer> queue = new LinkedList<>(asList(1, 2, 3, 4, 5, 6));
        Mono<Integer> mono = failSmallerThan(5, getMonoFromSupplier(queue));

        // when
        assertThatThrownBy(() -> mono.retry(3).block()).isInstanceOf(RuntimeException.class);

        // then
        assertThat(queue).hasSize(2);
    }

    private Mono<Integer> getMonoFromSupplier(LinkedList<Integer> queue) {
        return Mono.fromSupplier(queue::removeFirst);
    }

    private Mono<Integer> failSmallerThan(int min, Mono<Integer> mono) {
        return mono
            .map(i -> {
                if (i < min) {
                    throw new RuntimeException("too low: %d".formatted(i));
                }
                return i;
            });
    }
}
