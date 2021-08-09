package io.github.tuannh982.phantom.db;

import io.github.tuannh982.phantom.db.command.GetResult;
import io.github.tuannh982.phantom.db.command.ModifyResult;
import io.github.tuannh982.phantom.db.command.WriteOps;
import io.github.tuannh982.phantom.db.policy.WritePolicy;

import java.io.Closeable;
import java.io.IOException;

public interface DB extends Closeable {
    GetResult get(byte[] key) throws IOException;
    ModifyResult put(byte[] key, byte[] value) throws IOException;
    ModifyResult putIfAbsent(byte[] key, byte[] value) throws IOException;
    ModifyResult replace(byte[] key, byte[] value) throws IOException;
    ModifyResult delete(byte[] key) throws IOException;
    ModifyResult write(WriteOps ops, WritePolicy policy, byte[] key, byte[] value) throws IOException;
}
