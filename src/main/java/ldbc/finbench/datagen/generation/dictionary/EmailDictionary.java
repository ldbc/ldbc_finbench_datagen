package ldbc.finbench.datagen.generation.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class EmailDictionary {
    private final List<String> emails;
    private final List<Double> cumulativeDistribution;

    public EmailDictionary(String filePath, String separator) {
        try {
            BufferedReader emailDictionary = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(filePath), "UTF-8"));

            emails = new ArrayList<>();
            cumulativeDistribution = new ArrayList<>();

            String line;
            double cummulativeDist = 0.0;
            while ((line = emailDictionary.readLine()) != null) {
                String[] data = line.split(separator);
                emails.add(data[0]);
                if (data.length == 2) {
                    cummulativeDist += Double.parseDouble(data[1]);
                    cumulativeDistribution.add(cummulativeDist);
                }
            }
            emailDictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getRandomEmail(Random randomTop, Random randomEmail) {
        int minIdx = 0;
        int maxIdx = cumulativeDistribution.size() - 1;
        double prob = randomTop.nextDouble();
        if (prob > cumulativeDistribution.get(maxIdx)) {
            int Idx = randomEmail.nextInt(emails.size() - cumulativeDistribution.size()) + cumulativeDistribution
                .size();
            return emails.get(Idx);
        } else if (prob < cumulativeDistribution.get(minIdx)) {
            return emails.get(minIdx);
        }

        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return emails.get(maxIdx);
    }
}
