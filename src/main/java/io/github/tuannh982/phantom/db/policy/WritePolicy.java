package io.github.tuannh982.phantom.db.policy;

import io.github.tuannh982.phantom.commons.tuple.Tuple3;
import io.github.tuannh982.phantom.db.command.WriteOps;
import lombok.Builder;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Builder(builderClassName = "Builder", buildMethodName = "build")
public class WritePolicy extends Policy {
    public enum SequenceNumberPolicy {
        NONE,
        EXPECT_EQUAL,
    }

    public enum RecordExistsAction {
        NONE, // create or replace if put(), delete only if delete
        CREATE_ONLY,
        REPLACE_ONLY
    }

    private static final Set<Tuple3<WriteOps, SequenceNumberPolicy, RecordExistsAction>> combination;

    static {
        combination = new HashSet<>();
        combination.add(Tuple3.of(WriteOps.PUT, SequenceNumberPolicy.NONE, RecordExistsAction.NONE));
        combination.add(Tuple3.of(WriteOps.PUT, SequenceNumberPolicy.NONE, RecordExistsAction.CREATE_ONLY));
        combination.add(Tuple3.of(WriteOps.PUT, SequenceNumberPolicy.NONE, RecordExistsAction.REPLACE_ONLY));
        combination.add(Tuple3.of(WriteOps.PUT, SequenceNumberPolicy.NONE, RecordExistsAction.NONE));
        combination.add(Tuple3.of(WriteOps.PUT, SequenceNumberPolicy.EXPECT_EQUAL, RecordExistsAction.REPLACE_ONLY));
        combination.add(Tuple3.of(WriteOps.DELETE, SequenceNumberPolicy.NONE, RecordExistsAction.NONE));
        combination.add(Tuple3.of(WriteOps.DELETE, SequenceNumberPolicy.EXPECT_EQUAL, RecordExistsAction.NONE));
    }

    public static boolean combinable(WriteOps ops, SequenceNumberPolicy sequenceNumberPolicy, RecordExistsAction recordExistsAction) {
        return combination.contains(Tuple3.of(ops, sequenceNumberPolicy, recordExistsAction));
    }

    private final Long expectedSequenceNumber;
    private final SequenceNumberPolicy sequenceNumberPolicy;
    private final RecordExistsAction recordExistsAction;

    public static class Builder {
        void validate(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException("validation failed at: " + message);
            }
        }

        public WritePolicy build() {
            validate(
                    sequenceNumberPolicy == SequenceNumberPolicy.EXPECT_EQUAL && expectedSequenceNumber != null,
                    "expectedSequenceNumber must be existed while sequenceNumberPolicy = EXPECT_EQUAL"
            );
            return new WritePolicy(expectedSequenceNumber, sequenceNumberPolicy, recordExistsAction);
        }
    }
}
