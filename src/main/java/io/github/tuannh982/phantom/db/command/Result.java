package io.github.tuannh982.phantom.db.command;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public abstract class Result {
    private final Long sequenceNumber;
}
