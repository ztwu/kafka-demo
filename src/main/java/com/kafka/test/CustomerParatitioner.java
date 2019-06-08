package com.kafka.test;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomerParatitioner implements Partitioner {

    public void configure(Map<String, ?> configs) {
    }

    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        //key不能空   在源码中 key为空的会通过轮询的方式 选择分区
        if(keyBytes == null || (!(key instanceof String))){
            throw new RuntimeException("key is null");
        }
        //这里通过主题名称得到该主题所有的分区信息
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartition = partitionInfos.size();
        if(numPartition <= 1){
            return 0;
        }
        //后面进行逻辑判断返回哪个分区, 这里比如 key为123的 选择放入最后一个分区
        if(key.toString().equals("123")){
            return partitionInfos.size()-1;
        }
        //返回源码中默认的 按照原地址散列
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartition - 1));
    }

    public void close() {

    }

}