package com.kafka.test.v2;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicApi {

    /**
     * 配置并创建AdminClient
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        // 配置Kafka服务的访问地址及端口号
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.56.101:9092");

        // 创建AdminClient实例
        return AdminClient.create(properties);
    }

    /**
     * 创建topic
     */
    public static void createTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        // topic的名称
        String name = "MyTopic3";
        // partition数量
        int numPartitions = 1;
        // 副本数量
        short replicationFactor = 1;
        NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic[]{topic}));
        // 避免客户端连接太快断开而导致Topic没有创建成功
        Thread.sleep(500);
        // 获取topic设置的partition数量
        System.out.println(result.numPartitions(name).get());
    }

    /**
     * 查询Topic列表
     */
    public static void topicLists() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsResult result1 = adminClient.listTopics();
        // 打印Topic的名称
        System.out.println(result1.names().get());
        // 打印Topic的信息
        System.out.println(result1.listings().get());

        ListTopicsOptions options = new ListTopicsOptions();
        // 是否列出内部使用的Topic
        options.listInternal(true);
        ListTopicsResult result2 = adminClient.listTopics(options);
        System.out.println(result2.names().get());
    }

    /**
     * 删除Topic
     */
    public static void delTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(new String[]{"MyTopic"}));
        System.out.println(result.all().get());
    }

    /**
     * 查询Topic的描述信息
     */
    public static void describeTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(new String[]{"MyTopic3"}));
        Map<String, TopicDescription> descriptionMap = result.all().get();
        descriptionMap.forEach((key, value) ->
                System.out.println("name: " + key + ", desc: " + value));
    }

    /**
     * 查询Topic的配置信息
     */
    public static void describeConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC, "MyTopic3"
        );
        DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(new ConfigResource[]{configResource}));
        Map<ConfigResource, Config> map = result.all().get();
        map.forEach((key, value) ->
                System.out.println("name: " + key.name() + ", desc: " + value));
    }

    /**
     * 修改Topic的配置信息
     */
    public static void alterConfig() throws Exception {
        // 指定ConfigResource的类型及名称
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC, "MyTopic"
        );
        // 配置项以ConfigEntry形式存在
        Config config = new Config(Arrays.asList(new ConfigEntry[]
                {new ConfigEntry("preallocate", "true")}
                ));

        AdminClient adminClient = adminClient();
        Map<ConfigResource, Config> configMaps = new HashMap<>();
        configMaps.put(configResource, config);
        AlterConfigsResult result = adminClient.alterConfigs(configMaps);
        System.out.println(result.all().get());
    }

    /**
     * 修改Topic的配置信息
     */
    public static void incrementalAlterConfig() throws Exception {
        // 指定ConfigResource的类型及名称
        ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC, "MyTopic"
        );
        // 配置项同样以ConfigEntry形式存在，只不过增加了操作类型
        // 以及能够支持操作多个配置项，相对来说功能更多、更灵活
        // retention.ms 数据过期时间, 86400000 为一天，单位是毫秒。默认7天
        // 清理超过指定时间清理：
        // log.retention.hours=16
        // 超过指定大小后，删除旧的消息
        // log.retention.bytes=1073741824
        Collection<AlterConfigOp> configs = Arrays.asList(new AlterConfigOp[]{
                        new AlterConfigOp(
                                new ConfigEntry("preallocate", "false"),
                                AlterConfigOp.OpType.SET
                        )
                });

        AdminClient adminClient = adminClient();
        Map<ConfigResource, Collection<AlterConfigOp>> configMaps = new HashMap<>();
        configMaps.put(configResource, configs);
        AlterConfigsResult result = adminClient.incrementalAlterConfigs(configMaps);
        System.out.println(result.all().get());
    }

    /**
     * 增加Partition数量，目前Kafka不支持删除或减少Partition
     */
    public static void incrPartitions() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        // 将MyTopic的Partition数量调整为2
        newPartitions.put("MyTopic", NewPartitions.increaseTo(2));
        CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
        System.out.println(result.all().get());
    }

    public static void main(String[] args) {
        try {
//            TopicApi.createTopic();
            TopicApi.describeTopics();
            TopicApi.describeConfig();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
