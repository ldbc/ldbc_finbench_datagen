package ldbc.finbench.datagen.generation.distribution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import ldbc.finbench.datagen.generation.DatagenParams;

public class PowerLawBucketsDistribution extends DegreeDistribution {

    private List<Bucket> buckets;
    private List<Random> randomDegree;
    private Random randomPercentile;
    private int mean = 0;
    private static final int FB_MEAN = 190;

    public void initialize() {
        mean = (int) mean(DatagenParams.numPersons + DatagenParams.numCompanies);
        buckets = new ArrayList<>();
        loadFBBuckets();
        rebuildBucketRange();
        randomPercentile = new Random(DatagenParams.defaultSeed);
        randomDegree = new ArrayList<>();
        for (int i = 0; i < buckets.size(); i++) {
            randomDegree.add(new Random(DatagenParams.defaultSeed));
        }
    }

    private void loadFBBuckets() {
        try {
            BufferedReader fbDataReader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(DatagenParams.fbPowerlawDegreeFile),
                                      StandardCharsets.UTF_8));
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

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < buckets.size(); i++) {
            randomDegree.get(i).setSeed(seedRandom.nextLong());
        }
        randomPercentile.setSeed(seedRandom.nextLong());
    }

    public long nextDegree() {
        int idx = randomPercentile.nextInt(buckets.size());
        double minRange = (buckets.get(idx).min());
        double maxRange = (buckets.get(idx).max());
        if (maxRange < minRange) {
            maxRange = minRange;
        }
        return randomDegree.get(idx).nextInt((int) maxRange - (int) minRange + 1) + (int) minRange;
    }


    @Override
    public double mean(long numPersons) {
        return Math.round(Math.pow(numPersons, (0.512 - 0.028 * Math.log10(numPersons))));
    }
}
