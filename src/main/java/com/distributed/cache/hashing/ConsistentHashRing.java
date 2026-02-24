package com.distributed.cache.hashing;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRing {
    private final ConcurrentSkipListMap<Long, String> ring;
    private final int numberOfReplicas;
    private final HashFunction hashFunction;
    private final Set<String> nodes;

    public ConsistentHashRing(int numberOfReplicas) {
        this.ring = new ConcurrentSkipListMap<>();
        this.numberOfReplicas = numberOfReplicas;
        this.hashFunction = Hashing.murmur3_128();
        this.nodes = new HashSet<>();
    }

    public void addNode(String node) {
        if (nodes.add(node)) {
            for (int i = 0; i < numberOfReplicas; i++) {
                String virtualNode = node + "-" + i;
                long hash = hashFunction.hashBytes(virtualNode.getBytes()).asLong();
                ring.put(hash, node);
            }
        }
    }

    public void removeNode(String node) {
        if (nodes.remove(node)) {
            for (int i = 0; i < numberOfReplicas; i++) {
                String virtualNode = node + "-" + i;
                long hash = hashFunction.hashBytes(virtualNode.getBytes()).asLong();
                ring.remove(hash);
            }
        }
    }

    public String getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        long hash = hashFunction.hashBytes(key.getBytes()).asLong();
        if (!ring.containsKey(hash)) {
            Map.Entry<Long, String> entry = ring.higherEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            return entry.getValue();
        }
        return ring.get(hash);
    }

    public List<String> getBackupNodes(String primaryNode, int count) {
        List<String> allNodes = new ArrayList<>(nodes);
        allNodes.remove(primaryNode);
        return allNodes.subList(0, Math.min(count, allNodes.size()));
    }

    public Set<String> getAllNodes() {
        return Collections.unmodifiableSet(nodes);
    }

    public boolean containsNode(String node) {
        return nodes.contains(node);
    }
} 