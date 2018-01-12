package com.mighty.stormapps;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

/**
 * Created by z013sqm.
 * This class configures the HDFS Bolt configuration details and returns a configured HDFS Bolt
 */
public class RetrieveHdfsBolt {

    public static HdfsBolt getHdfsBolt(String hdfsuri, RecordFormat recFormat, FileNameFormat fileNameWithPath,
                                       FileRotationPolicy fileRotationSize, SyncPolicy syncPolicy){

    /*public static HdfsBolt getHdfsBolt(String hdfsuri, RecordFormat recFormat, FileNameFormat fileNameWithPath,
                                       FileRotationPolicy fileRotationSize, SyncPolicy syncPolicy, String secBypassConfigKey){*/


        //DatePartitioner dtPtnr = new DatePartitioner();

        HdfsBolt hdfsbolt = new HdfsBolt()
                .withFsUrl(hdfsuri)
                .withRecordFormat(recFormat)
                .withFileNameFormat(fileNameWithPath)
                .withRotationPolicy(fileRotationSize)
                .withSyncPolicy(syncPolicy);
                /*.withPartitioner(dtPtnr)
                .withConfigKey(secBypassConfigKey);*/

        return hdfsbolt;
    }
}