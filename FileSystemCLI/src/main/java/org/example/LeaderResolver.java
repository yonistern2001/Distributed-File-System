package org.example;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;

import java.util.concurrent.ExecutionException;

public class LeaderResolver {

    private static final ByteSequence KEY = ByteSequence.from("/election/control-service-leader".getBytes());
    private final KV kvClient;

    public LeaderResolver(Client client) {
        this.kvClient = client.getKVClient();
    }

    public String getLeader() throws ExecutionException, InterruptedException {

        GetResponse responce = this.kvClient.get(KEY).get();
        if (responce.getCount() == 0) {
            return null;
        }
        ByteSequence value = responce.getKvs().getFirst().getValue();
        return value.toString();
    }

    public void close() {
        this.kvClient.close();
    }
}
