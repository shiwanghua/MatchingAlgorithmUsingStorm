package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure;

public interface TypeConstant {
    // 操作类型
    int Insert_Subscription = 1;
    int Insert_Attribute_Subscription = 2;
    int Update_Attribute_Subscription = 3;
    int Delete_Attribute_Subscription = 4;
    int Delete_Subscription = 5;
    int Event_Match_Subscription = 6;
    int Null_Operation = 7;

    // 算法类型
    int SIMPLE = 1;
    int REIN = 2;
    int TAMA = 3;

    // 平凡参数
    int subSetSize = 10000;
    int numAttributeType = 30;
    int maxNumSubscriptionPerPacket = 1;
    int maxNumEventPerPacket = 1;
    int maxNumAttributePerSubscription = 10; // 对所有匹配算法都是越小越好！
//    String baseLogFilePath = "/home/swhua/Desktop/log/log1/";
    public static final String baseLogFilePath = "/home/swh/data/"; //"log_16_10b_3v16_2r/"; simple/log_15_3r/
    long intervalTime = 3000000000L; // The interval between two calculations of speed

    // 实验模型参数

    public static final int TYPE = 2; // 本次实验所用的匹配算法类型，根据这个参数生成合适的事件和订阅数据
    public static final int numWorkers=6;
    public static final int maxTaskParallelism=10; // >=numWorkers
    public static final int numAckers=3;

    // Simple
    public static final double maxAttributeProportion_Simple = 0.5;
    public static final int maxNumAttributePerEvent_Simple = (int) (numAttributeType * maxAttributeProportion_Simple);
    public static final double maxIntervalWidth_Simple = 0.2;

    //REIN
    public static final int numBucket = 10;
    public static final double minAttributeProportion_Rein = 1.0; // 事件有取值的属性个数至少占属性种数的比例
    public static final int minNumAttributePerEvent_Rein = (int) (numAttributeType * minAttributeProportion_Rein); // 每个事件里最少要有这么多个属性有值, 属性越少匹配越慢
    public static final double minIntervalWidth_Rein = 0.5;

    // TAMA
    public static final int numLevel = 11;
    public static final double maxAttributeProportion_Tama = 0.5; // 事件有取值的属性个数至多占属性种数的比例
    public static final int maxNumAttributePerEvent_Tama = (int) (numAttributeType * maxAttributeProportion_Tama);
    public static final double minIntervalWidth_Tama = 0.05;
    public static final double maxIntervalWidth_Tama = 0.5;

    //MPM
    public static final int numExecutorPerSpout = 1;
    public static final int numExecutorPerMatchBolt = 1;  // 并行度，用单个bolt作为一个并行算子组
    public static final int parallelismDegree = 6;       // 并行度，把单个bolt作为一个匹配器
    public static final int redundancy = 3;
    public static final int numMatchGroup = 2;
}

//public class TypeConstant {
//    // 操作类型
//    public static final int Insert_Subscription = 1;
//    public static final int Insert_Attribute_Subscription = 2;
//    public static final int Update_Attribute_Subscription = 3;
//    public static final int Delete_Attribute_Subscription = 4;
//    public static final int Delete_Subscription = 5;
//    public static final int Event_Match_Subscription = 6;
//    public static final int Null_Operation = 7;
//
//    // 算法类型
//    public static final int SIMPLE = 1;
//    public static final int REIN = 2;
//    public static final int TAMA = 3;
//
//    // 平凡参数
//    public static final int subSetSize = 100000;
//    public static final int numAttributeType = 30;
//    public static final int maxNumSubscriptionPerPacket = 1;
//    public static final int maxNumEventPerPacket = 1;
//    public static final int maxNumAttributePerSubscription = 10; // 对所有匹配算法都是越小越好！
//        public static final String baseLogFilePath = "/home/swhua/Desktop/log/log1/";
////    public static final String baseLogFilePath = "/root/log/tama/log_6_11l_5s_3r/"; //"log_16_10b_3v16_2r/"; simple/log_15_3r/
//    public static final long intervalTime = 60000000000L; // The interval between two calculations of speed
//
//    // 实验模型参数
//
//    public static final int TYPE = 3; // 本次实验所用的匹配算法类型，根据这个参数生成合适的事件和订阅数据
//
//    // Simple
//    public static final double maxAttributeProportion_Simple = 0.5;
//    public static final int maxNumAttributePerEvent_Simple = (int) (numAttributeType * maxAttributeProportion_Simple);
//    public static final double maxIntervalWidth_Simple = 0.2;
//
//    //REIN
//    public static final int numBucket = 10;
//    public static final double minAttributeProportion_Rein = 1.0; // 事件有取值的属性个数至少占属性种数的比例
//    public static final int minNumAttributePerEvent_Rein = (int) (numAttributeType * minAttributeProportion_Rein); // 每个事件里最少要有这么多个属性有值, 属性越少匹配越慢
//    public static final double minIntervalWidth_Rein = 0.5;
//
//    // TAMA
//    public static final int numLevel = 11;
//    public static final double maxAttributeProportion_Tama = 0.5; // 事件有取值的属性个数至多占属性种数的比例
//    public static final int maxNumAttributePerEvent_Tama = (int) (numAttributeType * maxAttributeProportion_Tama);
//    public static final double minIntervalWidth_Tama = 0.05;
//    public static final double maxIntervalWidth_Tama = 0.5;
//
//    //MPM
//    public static final int numExecutorPerSpout = 1;
//    public static final int numExecutorPerMatchBolt = 1; // 并行度，用单个bolt作为一个并行算子组
//    public static final int parallelismDegree = 6;       // 并行度，把单个bolt作为一个匹配器
//    public static final int redundancy = 3;
//    public static final int numMatchGroup = 1;
//}
