package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> ids = new ArrayList<>();
        for (Text value : values) {
            ids.add(value.toString());
        }
        context.write(key, new Text(String.join(",", ids)));
    }
}
