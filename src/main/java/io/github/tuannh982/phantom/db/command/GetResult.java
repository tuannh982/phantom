package io.github.tuannh982.phantom.db.command;

import lombok.Getter;

@Getter
public class GetResult extends Result {
    public static final GetResult NULL = new GetResult(null, null);

    private final byte[] value;

    public GetResult(byte[] value, Long sequenceNumber) {
        super(sequenceNumber);
        this.value = value;
    }
}
