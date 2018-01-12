package com.mighty.stormapps;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import kafka.api.OffsetRequest;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by z013sqm(Raja.Aravapalli).
 * This class configures the kafka configuration details and returns a configured KafkaSpout
 */
public class RetrieveAvroKafkaSpout {

    public static KafkaSpout getKafkaSpout(String kafkaConnStrings, String topicName, boolean frceFrmStrt, String topoName, String cnsmrGrpName){
        BrokerHosts hosts = new ZkHosts(kafkaConnStrings);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/storm" + "Basha_Storm_Tests" + "/" + topoName + "/" + topicName, cnsmrGrpName);

        spoutConfig.scheme = new SchemeAsMultiScheme(new RawScheme());

        //spoutConfig.forceFromStart = frceFrmStrt;

        if(frceFrmStrt == true){
            spoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
        }

        if(frceFrmStrt == false){
            spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
        }

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        return kafkaSpout;
    }

}