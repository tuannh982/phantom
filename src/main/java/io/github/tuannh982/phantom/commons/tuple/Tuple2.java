package io.github.tuannh982.phantom.commons.tuple;

import io.github.tuannh982.phantom.commons.assertion.Assertions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@SuppressWarnings({"java:S1301", "java:S131", "java:S119"})
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Tuple2<A0, A1> implements Tuple {
    private final A0 a0;
    private final A1 a1;

    @Override
    public Object get(int index) {
        Assertions.inRangeChecks(index, 0, 2);
        switch (index) {
            case 0:
                return a0;
            case 1:
                return a1;
        }
        throw new ArrayIndexOutOfBoundsException();
    }

    public static <AS0, AS1> Tuple2<AS0, AS1> of(AS0 arg0, AS1 arg1) {
        return new Tuple2<>(arg0, arg1);
    }
}
