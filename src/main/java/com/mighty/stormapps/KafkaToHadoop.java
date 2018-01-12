package com.mighty.stormapps;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.KafkaSpout;

import java.io.IOException;

/**
 * Created by z013sqm on 1/12/18.
 */
public class KafkaToHadoop {

    //Storm
    private static String topoName = null;
    private static boolean topoDebugFlag;
    private static int topoNumWorkers;
    private static int topoMaxSpoutPending;
    private static int topoNumAckers;
    private static int topoMsgTimeoutSecs;

    //Kafka
    private static String zkHosts = null;
    private static String kafkaTopic = null;
    private static boolean frceFrmStart;
    private static String cnsmrGroupName = null;
    private static String schemaRegUrl = null;

    //HDFS
    private static String hdfsUri = null;
    private static String hdfsFieldDelimit = null;
    private static String hdfsDestPath = null;
    private static float hdfsFileRotateSize;
    private static int countSyncPol;


    public static void main(String[]args) throws IOException, InvalidTopologyException,
            AlreadyAliveException, AuthorizationException {

        //Kafka Spout Setup
        KafkaSpout avroKafkaSpout = RetrieveAvroKafkaSpout.getKafkaSpout(zkHosts, kafkaTopic, frceFrmStart, topoName, cnsmrGroupName);

        //HDFS Bolt setup
        FileNameFormat fileNameWithPath = new DefaultFileNameFormat().withPath(hdfsDestPath);
        RecordFormat recFormat = new DelimitedRecordFormat().withFieldDelimiter(hdfsFieldDelimit);
        SyncPolicy syncPolicy = new CountSyncPolicy(countSyncPol);
        FileRotationPolicy fileRotatePolicy = new FileSizeRotationPolicy(hdfsFileRotateSize, FileSizeRotationPolicy.Units.MB);

        //HdfsBolt avroHDFSBolt = RetrieveHdfsBolt.getHdfsBolt(hdfsUri, recFormat, fileNameWithPath, fileRotatePolicy, syncPolicy, hdfsConfigKey);
        HdfsBolt avroHDFSBolt = RetrieveHdfsBolt.getHdfsBolt(hdfsUri, recFormat, fileNameWithPath, fileRotatePolicy, syncPolicy);

        //Storm config setup
        Config config = new Config();
        //Configuring config object
        config.setDebug(topoDebugFlag);
        config.setNumWorkers(topoNumWorkers);
        config.setMaxSpoutPending(topoMaxSpoutPending);
        config.setNumAckers(topoNumAckers);
        config.setMessageTimeoutSecs(topoMsgTimeoutSecs);

        //Configuring topology builder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("avro-kafka-spout", avroKafkaSpout);

        builder.setBolt("write-visit-info", avroHDFSBolt)
                //.setNumTasks(topoBoltWriteVisitInfoTasksNum)
                .shuffleGrouping("session-check-and-update");

        //create and submit topology
        StormTopology topology = builder.createTopology();
        StormSubmitter.submitTopology(topoName, config, topology);

    }


}
