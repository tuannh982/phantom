package io.github.tuannh982.phantom.db.command;

import lombok.Getter;

@Getter
public class ModifyResult extends Result {
    /**
     * - put:           + return new sequenceNumber & success = true if success
     * - putIfAbsent:   + return new sequenceNumber & success = true if success
     *                  + return old sequenceNumber & success = false if record already existed
     * - replace:       + return sequenceNumber = null & success = false if record not existed
     *                  + return new sequenceNumber & success = true if success
     * - replaceWithSequenceNumberEquals:       + sequenceNumber = null & success = false if record not existed
     *                                          + sequenceNumber = old sequenceNumber & success = false
     *                                          if record already existed and expected sequenceNumber not matched
     *                                          + return new sequenceNumber & success = true if success
     * - delete:        + return new sequenceNumber & success = true if success
     *                  + return sequenceNumber = null & success = false if record not existed
     * - deleteWithSequenceNumberEquals:        + return new sequenceNumber & success = true if success
     *                                          (with record already existed and expected sequenceNumber are matched)
     *                                          + return sequenceNumber = null & success = false if record not existed
     */
    public static final ModifyResult FAILED = new ModifyResult(false, null);

    private final boolean success;

    public ModifyResult(boolean success, Long sequenceNumber) {
        super(sequenceNumber);
        this.success = success;
    }
}
