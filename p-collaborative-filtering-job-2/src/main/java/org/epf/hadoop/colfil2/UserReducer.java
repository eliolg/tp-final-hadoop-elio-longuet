package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UserReducer extends Reducer<UserPair, Text, UserPair, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(UserPair pair, Iterable<Text> sharedUsers, Context context) throws IOException, InterruptedException {
        HashSet<String> userSet = new HashSet<>();
        boolean hasDirectConnection = false;

        for (Text user : sharedUsers) {
            if (userSet.contains(user.toString())) {
                hasDirectConnection = true;
                break;
            }
            userSet.add(user.toString());
        }

        if (!hasDirectConnection && userSet.size() > 1) {
            outputValue.set(String.valueOf(userSet.size()));
            context.write(pair, outputValue);
        }
    }
}

