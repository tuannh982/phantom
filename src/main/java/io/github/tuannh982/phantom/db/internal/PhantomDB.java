package io.github.tuannh982.phantom.db.internal;

import io.github.tuannh982.phantom.db.DB;
import io.github.tuannh982.phantom.db.DBException;
import io.github.tuannh982.phantom.db.command.GetResult;
import io.github.tuannh982.phantom.db.command.ModifyResult;
import io.github.tuannh982.phantom.db.command.WriteOps;
import io.github.tuannh982.phantom.db.policy.WritePolicy;

import java.io.File;
import java.io.IOException;

public class PhantomDB implements DB {
    private final PhantomDBInternal internal;

    public PhantomDB(File dir, PhantomDBOptions options) throws IOException, DBException {
        if (!dir.isDirectory()) {
            throw new AssertionError(dir.getName() + " is not a directory");
        }
        internal = PhantomDBInternal.open(dir, options);
    }

    @Override
    public GetResult get(byte[] key) throws IOException {
        return internal.get(key);
    }

    @Override
    public ModifyResult put(byte[] key, byte[] value) throws IOException {
        return internal.put(key, value);
    }

    @Override
    public ModifyResult putIfAbsent(byte[] key, byte[] value) throws IOException {
        return internal.putIfAbsent(key, value);
    }

    @Override
    public ModifyResult replace(byte[] key, byte[] value) throws IOException {
        return internal.replace(key, value);
    }

    @Override
    public ModifyResult delete(byte[] key) throws IOException {
        return internal.delete(key);
    }

    @SuppressWarnings("java:S3776")
    @Override
    public ModifyResult write(WriteOps ops, WritePolicy policy, byte[] key, byte[] value) throws IOException {
        WritePolicy.SequenceNumberPolicy sequenceNumberPolicy = policy.getSequenceNumberPolicy();
        WritePolicy.RecordExistsAction recordExistsAction = policy.getRecordExistsAction();
        if (WritePolicy.combinable(ops, sequenceNumberPolicy, recordExistsAction)) {
            if (ops == WriteOps.PUT) {
                if (sequenceNumberPolicy == WritePolicy.SequenceNumberPolicy.NONE) {
                    if (recordExistsAction == WritePolicy.RecordExistsAction.NONE) {
                        return internal.put(key, value);
                    } else if (recordExistsAction == WritePolicy.RecordExistsAction.CREATE_ONLY) {
                        return internal.putIfAbsent(key, value);
                    } else if (recordExistsAction == WritePolicy.RecordExistsAction.REPLACE_ONLY) {
                        return internal.replace(key, value);
                    }
                } else if (sequenceNumberPolicy == WritePolicy.SequenceNumberPolicy.EXPECT_EQUAL) {
                    return internal.replaceWithSequenceNumberEquals(key, value, policy.getExpectedSequenceNumber());
                }
            } else if (ops == WriteOps.DELETE) {
                if (sequenceNumberPolicy == WritePolicy.SequenceNumberPolicy.NONE) {
                    return internal.delete(key);
                } else if (sequenceNumberPolicy == WritePolicy.SequenceNumberPolicy.EXPECT_EQUAL) {
                    return internal.deleteWithSequenceNumberEquals(key, policy.getExpectedSequenceNumber());
                }
            }
        }
        return ModifyResult.FAILED;
    }

    @Override
    public void close() throws IOException {
        internal.close();
    }
}
