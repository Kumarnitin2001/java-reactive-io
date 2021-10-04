package io.github.kn.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
class OptionalsTest {

    @BeforeEach
    void setUp() {

    }

    @Test
    void create() {
        Assertions.assertEquals("123", Optionals.create(List.of(1, 2, 3)).toString());
    }

    @Test
    void from() {
        Assertions.assertEquals("13", Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .toString());
    }


    @Test
    void replace() {
        Assertions.assertEquals("45", Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .replace(List.of(4, 5))
                .toString());
    }

    @Test
    void mergeEmpty() {
        Assertions.assertEquals("", Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .mergeEmpties(Optionals
                        .wrap(Optionals
                                .create(List.of(Optional.empty(), Optional.ofNullable(3), Optional.empty()))))
                .toString());
    }

    @Test
    void mergeEmptyDifferentSizes() {
        Assertions.assertEquals("", Optionals
                .wrap(Optionals.create(List
                        .of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3), Optional.empty())))
                .mergeEmpties(Optionals
                        .wrap(Optionals
                                .create(List.of(Optional.empty(), Optional.ofNullable(3), Optional.empty()))))
                .toString());
    }

    @Test
    void map() {
        Assertions.assertEquals("mapped1mapped3", Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .map(i -> "mapped" + i).toString());
    }

    @Test
    void stream() {
        Assertions.assertEquals("[1, 3]", Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .stream().collect(Collectors.toList()).toString());
    }

    @Test
    void iterator() {
        Iterator<Integer> it = Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))))
                .iterator();
        Assertions.assertEquals(1, it.next());
        Assertions.assertEquals(3, it.next());
        Assertions.assertFalse(it.hasNext());
    }

    @Test
    void shallowCopy() {
        ArrayList<Optional<Integer>> list = new ArrayList<>(List
                .of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3)));
        Optionals<Integer> optionals = Optionals.wrap(Collections.unmodifiableList(list));
        Assertions.assertEquals("13", optionals.toString());
        list.remove(0);
        Assertions.assertEquals("3", optionals.toString());
    }


    @Test
    void repeatIterator() {
        Optionals<Integer> optionals = Optionals
                .wrap(Optionals.create(List.of(Optional.ofNullable(1), Optional.empty(), Optional.ofNullable(3))));
        Iterator<Integer> it = optionals.iterator();
        Assertions.assertEquals(1, it.next());
        Assertions.assertEquals(3, it.next());

        it = optionals.iterator();
        Assertions.assertEquals(1, it.next());
        Assertions.assertEquals(3, it.next());
    }

}