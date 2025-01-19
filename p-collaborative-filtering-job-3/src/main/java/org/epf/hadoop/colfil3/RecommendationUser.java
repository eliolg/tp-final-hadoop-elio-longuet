package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecommendationUser implements WritableComparable<RecommendationUser> {
    private String sourceUserId;
    private String targetUserId;
    private int mutualFriendsCount;

    public RecommendationUser() {
        this.sourceUserId = "";
        this.targetUserId = "";
        this.mutualFriendsCount = 0;
    }

    public RecommendationUser(String sourceUserId, String targetUserId, int mutualFriendsCount) {
        this.sourceUserId = sourceUserId;
        this.targetUserId = targetUserId;
        this.mutualFriendsCount = mutualFriendsCount;
    }

    public String getSourceUserId() { return sourceUserId; }
    public String getTargetUserId() { return targetUserId; }
    public int getMutualFriendsCount() { return mutualFriendsCount; }

    @Override
    public void readFields(DataInput in) throws IOException {
        sourceUserId = in.readUTF();
        targetUserId = in.readUTF();
        mutualFriendsCount = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sourceUserId);
        out.writeUTF(targetUserId);
        out.writeInt(mutualFriendsCount);
    }

    @Override
    public int compareTo(RecommendationUser other) {
        int comparison = -Integer.compare(this.mutualFriendsCount, other.mutualFriendsCount);
        return (comparison != 0) ? comparison : this.targetUserId.compareTo(other.targetUserId);
    }

    @Override
    public String toString() {
        return targetUserId + "(" + mutualFriendsCount + ")";
    }
}
