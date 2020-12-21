//package com.kafka.test;
//
//import java.util.List;
//import java.util.Properties;
//
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.Node;
//import org.apache.kafka.common.PartitionInfo;
//import org.apache.kafka.common.security.JaasUtils;
//
//import kafka.admin.AdminUtils;
//import kafka.admin.BrokerMetadata;
//import kafka.server.ConfigType;
//import kafka.utils.ZkUtils;
//import scala.collection.Seq;
//
///**
// * created with idea
// * user:ztwu
// * date:2019/5/28
// * description
// */
//public class TopicDemo {
//
//    private static final String ZOOKEEPER_CONNECT = "192.168.56.101:2181";
//    private static final int SESSION_TIMEOUT = 1000 * 30; // 与 zookeeper 连接的 session 的过期时间
//    private static final int CONNECT_TIMEOUT = 5000; // 连接 zookeeper 的超时时间
//
//    public static void main(String[] args){
////        createTopic("ztwu300",1,1);
//        addPartitions("192.168.56.101:9092","ztwu300",1,"1");
////        reallocateReplica("ztwu200",2,2);
//    }
//
//    /**
//     * 获取ZkUtils
//     */
//    private static ZkUtils getZkUtils(String zookeeperConnect, int sessionTimeout, int connectTimeout) {
//        ZkUtils zkUtils = null;
//        try {
//            zkUtils = ZkUtils.apply(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
//        } catch (Exception e) {
//            System.err.println("initialize zkUtils failed!");
//            e.printStackTrace();
//        }
//        return zkUtils;
//    }
//
//    /**
//     * 创建topic并且手动配置一些参数
//     * @param topic topicName
//     * @param numPartitions 分区数
//     * @param numReplications 副本数
//     * @param properties 配置，设置后可以覆盖默认配置
//     */
//    public static void createTopic(String topic, int numPartitions, int numReplications, Properties properties) {
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            if(!AdminUtils.topicExists(zkUtils, topic)) {
//                AdminUtils.createTopic(zkUtils, topic, numPartitions, numReplications, properties, AdminUtils.createTopic$default$6());
//            } else {
//                // TODO log
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if(zkUtils != null) {
//                zkUtils.close();
//            }
//        }
//    }
//
//    /**
//     * 创建默认配置的主题
//     */
//    public static void createTopic(String topic, int numPartitions, int numReplications) {
//        createTopic(topic, numPartitions, numReplications, new Properties());
//    }
//
//    /**
//     * 删除topic
//     */
//    public static void deleteTopic(String topic) {
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            AdminUtils.deleteTopic(zkUtils, topic);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            zkUtils.close();
//        }
//    }
//
//    /**
//     * 修改主题级别的配置
//     * @param topic
//     * @param properties 新的配置
//     */
//    public static void modifyTopicConfig(String topic, Properties properties) {
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            // 获取当前已有的配置，这里是查询主题级别的配置，因此指定配置类型为ConfigType.Topic()
//            Properties currentConfig = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
//            currentConfig.putAll(properties);
//            AdminUtils.changeTopicConfig(zkUtils, topic, currentConfig);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            zkUtils.close();
//        }
//    }
//
//    /**
//     * 删除某个主题级别的配置
//     * @param topic
//     * @param key 要删除的配置项
//     */
//    public static void deleteTopicConfig(String topic, String key) {
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            Properties currentConfig = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
//            currentConfig.remove(key);
//            AdminUtils.changeTopicConfig(zkUtils, topic, currentConfig);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            zkUtils.close();
//        }
//    }
//
//    /**
//     * 增加分区
//     * @param topic
//     * @param numAddPartitions 要增加的分区个数
//     * @param replicaAssignmentStr 分区副本分配的策略
//     * 如果要添加2个分区，那么这个字符串的格式为"x:y,x:z"，例如"1:2,2:3"的意思是：
//     * 添加两个分区，添加的第一个分区的副本在1和2这两个broker上，添加的第二个分区的副本在2和3这两个broker上
//     * 假如要添加3个分区，每个分区有3个副本，那么，这个字符串的格式为"x:y:z,x:z:y,z:x:y"
//     */
//    public static void addPartitions(String brokerList, String topic, int numAddPartitions, String replicaAssignmentStr) {
//
//        List<PartitionInfo> partitionInfos = getTopicMetadata(brokerList, topic);
//
//        StringBuilder sb = new StringBuilder();
//        for(PartitionInfo info : partitionInfos) {
//            Node[] replicas = info.replicas();
//            for(int i = 0; i < replicas.length; i++) {
//                sb.append(replicas[i].id()).append(":");
//                if(i == replicas.length - 1) {
//                    String str = sb.substring(0, sb.length() - 1);
//                    sb = new StringBuilder(str);
//                    sb.append(",");
//                }
//            }
//        }
//
//        String replicaAssignmentStr2 = sb.toString() + replicaAssignmentStr;
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            AdminUtils.addPartitions(
//                    zkUtils,
//                    topic,
//                    partitionInfos.size() + numAddPartitions,
//                    replicaAssignmentStr2,
//                    true,
//                    AdminUtils.addPartitions$default$6());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            zkUtils.close();
//        }
//
//    }
//
//    /**
//     * 分区副本重分配
//     * @param topic
//     * @param numTotalPartitions 重分区后的分区数
//     * @param numReplicasPerPartition 重分区后每个分区的副本数
//     */
//    public static void reallocateReplica(String topic, int numTotalPartitions, int numReplicasPerPartition) {
//        ZkUtils zkUtils = getZkUtils(ZOOKEEPER_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
//        try {
//            // 获取broker原数据信息
//            scala.collection.Seq<BrokerMetadata> brokerMetadata = AdminUtils.getBrokerMetadatas(zkUtils,
//                    AdminUtils.getBrokerMetadatas$default$2(),
//                    AdminUtils.getBrokerMetadatas$default$3());
//            // 生成新的分区副本分配方案
//            scala.collection.Map<Object, Seq<Object>> replicaAssign = AdminUtils.assignReplicasToBrokers(brokerMetadata,
//                    numTotalPartitions, // 分区数
//                    numReplicasPerPartition, // 每个分区的副本数
//                    AdminUtils.assignReplicasToBrokers$default$4(),
//                    AdminUtils.assignReplicasToBrokers$default$5());
//            // 修改分区副本分配方案
//            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils,
//                    topic,
//                    replicaAssign,
//                    null,
//                    true);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            zkUtils.close();
//        }
//    }
//
//    /**
//     * 获取主题元数据
//     */
//    public static List<PartitionInfo> getTopicMetadata(String brokerList, String topic) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", brokerList);
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
//        consumer.close();
//        return partitionInfos;
//    }
//
//    /**
//     * 在控制台打印List<PartitionInfo>
//     */
//    public static void printTopicDescribe(List<PartitionInfo> partitionInfos) {
//        for(PartitionInfo info : partitionInfos) {
//            Node[] replicas = info.replicas();
//            Node[] isr = info.inSyncReplicas();
//            StringBuilder result1 = new StringBuilder();
//            result1.append("Topic: ").append(info.topic()).append("\t");
//            result1.append("Partition: ").append(info.partition()).append("\t");
//            result1.append("Leader: ").append(info.leader().id()).append("\t");
//            result1.append("Replicas: ");
//            for(Node node : replicas) {
//                result1.append(node.id()).append(",");
//            }
//            String str1 = result1.substring(0, result1.length() - 1);
//            StringBuilder result2 = new StringBuilder(str1);
//            result2.append("\t").append("Isr: ");
//            for(Node node : isr) {
//                result2.append(node.id()).append(",");
//            }
//            String str2 = result2.substring(0, result2.length() - 1);
//            System.out.println(str2);
//        }
//    }
//}
