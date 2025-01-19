package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, RecommendationUser> {
        private Text userKey = new Text();
    
        @Override
        protected void map(LongWritable inputKey, Text record, Context context)
                throws IOException, InterruptedException {
    
            String[] lineParts = record.toString().split("\t");
            if (lineParts.length != 2) return;
    
            String[] userPair = lineParts[0].split("\\.");
            if (userPair.length != 2) return;
    
            int mutualFriends = Integer.parseInt(lineParts[1]);
    
            userKey.set(userPair[0]);
            context.write(userKey, new RecommendationUser(
                    userPair[0], userPair[1], mutualFriends));
    
            userKey.set(userPair[1]);
            context.write(userKey, new RecommendationUser(
                    userPair[1], userPair[0], mutualFriends));
        }
    }
    