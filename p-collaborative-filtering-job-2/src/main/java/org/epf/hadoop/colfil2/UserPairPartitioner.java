package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPairPartitioner extends Partitioner<UserPair, Text> {
    @Override
    public int getPartition(UserPair pair, Text textValue, int numberOfReducers) {
        int primaryHash = pair.getFirstUser().hashCode();
        return Math.abs(primaryHash) % numberOfReducers;
    }
}

