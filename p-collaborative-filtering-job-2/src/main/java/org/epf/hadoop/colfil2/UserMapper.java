package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class UserMapper extends Mapper<LongWritable, Text, UserPair, Text> {

    private UserPair pair = new UserPair();
    private Text commonUser = new Text();

    @Override
    protected void map(LongWritable offset, Text record, Context context) throws IOException, InterruptedException {
        String lineContent = record.toString();
        String[] userAndConnections = lineContent.split("\t");

        if (userAndConnections.length < 2) return;

        String mainUser = userAndConnections[0];
        String[] connections = userAndConnections[1].split(",");

        Arrays.sort(connections);
        commonUser.set(mainUser);

        for (int i = 0; i < connections.length; i++) {
            for (int j = i + 1; j < connections.length; j++) {
                pair = new UserPair(connections[i], connections[j]);
                context.write(pair, commonUser);
            }
        }
    }
}

