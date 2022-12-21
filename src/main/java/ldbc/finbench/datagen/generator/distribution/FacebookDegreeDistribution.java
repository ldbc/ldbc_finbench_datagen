package ldbc.finbench.datagen.generator.distribution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import ldbc.finbench.datagen.generator.DatagenParams;
import ldbc.finbench.datagen.util.GeneratorConfiguration;

public class FacebookDegreeDistribution extends BucketedDistribution {
    private int mean = 0;
    private static final int FB_MEAN = 190;
    private List<Bucket> buckets;

    @Override
    public List<Bucket> getBuckets(GeneratorConfiguration conf) {
        mean = (int) mean(DatagenParams.numPersons);
        buckets = new ArrayList<>();
        loadFBBuckets();
        rebuildBucketRange();
        return buckets;
    }

    private void loadFBBuckets() {
        try {
            BufferedReader fbDataReader = new BufferedReader(new InputStreamReader(
                    getClass().getResourceAsStream(DatagenParams.facebookDegreeFile), StandardCharsets.UTF_8));
            String line;
            while ((line = fbDataReader.readLine()) != null) {
                String[] data = line.split(" ");
                buckets.add(new Bucket(Float.parseFloat(data[0]), Float.parseFloat(data[1])));
            }
            fbDataReader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void rebuildBucketRange() {
        double newMin;
        double newMax;
        for (Bucket bucket : buckets) {
            newMin = bucket.min() * mean / FB_MEAN;
            newMax = bucket.max() * mean / FB_MEAN;
            if (newMax < newMin) {
                newMax = newMin;
            }
            bucket.min(newMin);
            bucket.max(newMax);
        }
    }

    @Override
    public double mean(long numPersons) {
        return Math.round(Math.pow(numPersons, (0.512 - 0.028 * Math.log10(numPersons))));
    }
}
