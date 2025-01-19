package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, RecommendationUser, Text, Text> {
    private static final int MAX_RECOMMENDATIONS = 5;
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text userKey, Iterable<RecommendationUser> recommendations, Context context)
            throws IOException, InterruptedException {

        TreeSet<RecommendationUser> topRecommendations = new TreeSet<>((rec1, rec2) -> {
            int compareFriends = -Integer.compare(rec1.getMutualFriendsCount(), rec2.getMutualFriendsCount());
            return (compareFriends != 0) ? compareFriends : rec1.getTargetUserId().compareTo(rec2.getTargetUserId());
        });

        for (RecommendationUser recommendation : recommendations) {
            RecommendationUser recCopy = new RecommendationUser(
                    recommendation.getSourceUserId(),
                    recommendation.getTargetUserId(),
                    recommendation.getMutualFriendsCount()
            );
            topRecommendations.add(recCopy);

            if (topRecommendations.size() > MAX_RECOMMENDATIONS) {
                topRecommendations.pollLast();
            }
        }

        if (!topRecommendations.isEmpty()) {
            List<String> formattedRecommendations = new ArrayList<>();
            for (RecommendationUser topRec : topRecommendations) {
                formattedRecommendations.add(String.format("%s(%d)", topRec.getTargetUserId(), topRec.getMutualFriendsCount()));
            }
            outputValue.set(String.join(", ", formattedRecommendations));
            context.write(userKey, outputValue);
        } else {
            outputValue.set("No recommendations available");
            context.write(userKey, outputValue);
        }
    }
}
