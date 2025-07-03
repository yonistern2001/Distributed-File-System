package org.example.filesystem.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.example.filesystem.model.FileInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@Repository
public class EtcdRepository {

    private final KV kvClient;
    private final ObjectMapper objectMapper;

    public EtcdRepository(@Autowired Client client, @Autowired ObjectMapper objectMapper) {
        this.kvClient = client.getKVClient();
        this.objectMapper = objectMapper;
    }

    public String getOrSetLeader(String currNodeAddress) throws ExecutionException, InterruptedException {
        ByteSequence key = createLeaderKey();
        ByteSequence value = ByteSequence.from(currNodeAddress.getBytes());
        TxnResponse response = kvClient.txn()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
                .Then(Op.put(key, value, PutOption.DEFAULT))
                .Else(Op.get(key, GetOption.DEFAULT))
                .commit()
                .get();

        if (response.isSucceeded()) {
            return currNodeAddress;
        }

        return response.getGetResponses().getFirst().getKvs().getFirst().getValue().toString();
    }

    public void deleteLeader(String currLeaderAddress) throws ExecutionException, InterruptedException {
        kvClient.delete(createLeaderKey()).get();
        ByteSequence key = createLeaderKey();
        ByteSequence expectedValue = ByteSequence.from(currLeaderAddress.getBytes());
        kvClient.txn()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(expectedValue)))
                .Then(Op.delete(key, DeleteOption.DEFAULT))
                .commit()
                .get();
    }

    public void saveFileInfo(FileInfo fileInfo) throws ExecutionException, InterruptedException, JsonProcessingException {
        ByteSequence keyBytes = createFileInfoKey(fileInfo.getFilePath());
        ByteSequence valueBytes = serialize(fileInfo);
        kvClient.put(keyBytes, valueBytes).get();
    }

    public FileInfo getFileInfo(String path) throws ExecutionException, InterruptedException {
        ByteSequence keyBytes = createFileInfoKey(path);
        List<KeyValue> kvsResponse = kvClient.get(keyBytes).get().getKvs();

        if (kvsResponse.isEmpty()) {
            return null;
        }

        ByteSequence valueBytes = kvsResponse.getFirst().getValue();
        return deserialize(valueBytes);
    }

    public List<FileInfo> getFileInfosStartingWith(String pathPrefix) throws ExecutionException, InterruptedException {
        ByteSequence keyPrefix = createFileInfoKey(pathPrefix);
        List<KeyValue> response = kvClient.get(keyPrefix, GetOption.newBuilder().isPrefix(true).build()).get().getKvs();
        return response.stream().map(KeyValue::getValue).map(this::deserialize).toList();
    }

    public FileInfo deleteFileInfo(String path) throws ExecutionException, InterruptedException {
        ByteSequence keyBytes = createFileInfoKey(path);
        DeleteOption deleteOption = DeleteOption.newBuilder().withPrevKV(true).build();
        DeleteResponse deleteResponse = kvClient.delete(keyBytes, deleteOption).get();

        if(deleteResponse.getDeleted() == 0) {
            throw new NoSuchElementException();
        }

        ByteSequence deletedValue = deleteResponse.getPrevKvs().getFirst().getValue();
        return deserialize(deletedValue);
    }

    private ByteSequence serialize(FileInfo fileInfo) throws JsonProcessingException {
        return ByteSequence.from(objectMapper.writeValueAsBytes(fileInfo));
    }

    private FileInfo deserialize(ByteSequence byteSequence) {
        try {
            return objectMapper.readValue(byteSequence.getBytes(), FileInfo.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ByteSequence createFileInfoKey(String path) {
        return ByteSequence.from(("/files/" + path).getBytes());
    }

    private static ByteSequence createLeaderKey() {
        return ByteSequence.from("/election/control-service-leader".getBytes());
    }
}
