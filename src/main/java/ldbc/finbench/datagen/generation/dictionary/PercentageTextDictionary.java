package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PercentageTextDictionary {

    private final List<String> resources;
    private final List<Double> cumulativeDistribution;

    public PercentageTextDictionary(String filePath, String separator) {
        resources = new ArrayList<>();
        cumulativeDistribution = new ArrayList<>();

        try {
            BufferedReader dictionary = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8"));
            String line;
            double cummulativeDist = 0.0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(separator);
                String browser = data[0];
                cummulativeDist += Double.parseDouble(data[1]);
                resources.add(browser);
                cumulativeDistribution.add(cummulativeDist);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName(int id) {
        return resources.get(id);
    }

    public String getDistributedText(Random random) {
        double prob = random.nextDouble();
        int minIdx = 0;
        int maxIdx = (byte) ((prob < cumulativeDistribution.get(minIdx)) ? minIdx : cumulativeDistribution
            .size() - 1);
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return resources.get(maxIdx);
    }
}
