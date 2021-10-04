package io.github.kn.flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An iterable of {@link Optional} values.
 */
public class Optionals<E> implements Iterable<E> {
    private static final Logger LOG = LogManager.getLogger("Optionals");

    private final OptionalIterable<?, E> iterable;

    /**
     * @param delegate
     */
    private <V> Optionals(final Iterable<Optional<V>> delegate, final Function<V, E> func) {
        this(new OptionalIterable<>(func, delegate));
    }

    private Optionals(final OptionalIterable<?, E> input) {
        iterable = input;
    }


    private Optionals(final Iterable<Optional<E>> delegate) {
        this(delegate, Function.identity());
    }

    public static <V> Optionals<V> create(final Iterable<V> from) {
        return new Optionals<>(StreamSupport.stream(from.spliterator(), false).map(Optional::ofNullable)
                .collect(Collectors.toList()));
    }

    public static <V> Optionals<V> wrap(final Iterable<Optional<V>> from) {
        LOG.trace("wrap invoked with wrap :{}", from);
        return new Optionals<>(from);
    }

    /**
     * Replaces Non-Empty elements of this Optionals with the passed in Iterable elements, returning a new Optionals.
     *
     * @param to  iterables to replace with
     * @param <V> type of Optionals
     * @return A new Optionals after replace
     * @see #mergeEmpties(Optionals)
     */
    public <V> Optionals<V> replace(final Iterable<V> to) {
        LOG.trace("replace invoked with this :{} and to :{}", this, to);
        Iterator<V> toIter = to.iterator();
        return new Optionals<>(new OptionalIterable<>(o -> toIter.next(), this.iterable.rawIterable()).mappedStream()
                .collect(Collectors.toList()));
    }

    /**
     * Merges-in Empty elements of the passed in Optionals (replacing non empty values in this optional with empty
     * values, in the process), returning a new Optionals.
     *
     * @param from Optionals to merge empty values from
     * @param <V>  type of from Optionals
     * @return a new Optional after merge
     * @see #replace(Iterable)
     */
    public <V> Optionals<E> mergeEmpties(final Optionals<V> from) {
        LOG.trace("mergeEmpties invoked with this :{} and wrap :{}", this, from);
        Iterator<Optional<V>> fromIt = from.iterable.mappedStream().iterator();
        return new Optionals<>(this.iterable.mappedStream().
                map(s -> fromIt.hasNext() && fromIt.next().isEmpty() && s.isPresent() ? Optional.<E>empty() : s)
                .collect(Collectors.toList()));
    }

    /**
     * Apply the passes in mapper to values of this optionals
     *
     * @param mapper function to apply
     * @param <V>
     * @return a new optionals
     */
    public <V> Optionals<V> map(final Function<E, V> mapper) {
        LOG.trace("map invoked with this :{} and mapper:{}", this, mapper);
        return new Optionals<>(this.iterable.map(mapper));
    }

    public Stream<E> stream() {
        return StreamSupport.stream(this.iterable.spliterator(), false);
    }

    @Override
    public Iterator<E> iterator() {
        return this.iterable.iterator();
    }

    @Override
    public String toString() {
        return this.stream()
                .collect(
                        StringBuilder::new,
                        StringBuilder::append,
                        StringBuilder::append
                )
                .toString();
    }

    private static class OptionalIterable<V, E> implements Iterable<E> {
        private final Function<V, E> mapper;
        private final Iterable<Optional<V>> optionalIterable;

        private OptionalIterable(Function<V, E> mapper, Iterable<Optional<V>> iterable) {
            this.mapper = mapper;
            this.optionalIterable = iterable;
        }

        private Stream<Optional<E>> mappedStream() {
            return StreamSupport.stream(rawIterable().spliterator(), false).map(o -> o.map(mapper));
        }

        private Iterable<Optional<V>> rawIterable() {
            return optionalIterable;
        }

        private <R> OptionalIterable<V, R> map(final Function<E, R> func) {
            return new OptionalIterable<>(this.mapper.andThen(func), this.optionalIterable);
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<>() {
                private final Iterator<E> nonOptionalList =
                        mappedStream().filter(Optional::isPresent).map(Optional::get).iterator();

                @Override
                public boolean hasNext() {
                    return nonOptionalList.hasNext();
                }

                @Override
                public E next() {
                    return nonOptionalList.next();
                }

                @Override
                public String toString() {
                    return nonOptionalList.toString();
                }
            };
        }
    }
}
