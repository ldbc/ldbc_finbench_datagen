/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigParser {

    public static Map<String, String> readConfig(String paramsFile) {
        try (FileInputStream fis = new FileInputStream(paramsFile)) {
            return readConfig(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> readConfig(InputStream paramStream) {
        Properties properties = new Properties();
        Map<String, String> res = new HashMap<>();
        try {
            properties.load(new InputStreamReader(paramStream, StandardCharsets.UTF_8));
            for (String s : properties.stringPropertyNames()) {
                res.put(s, properties.getProperty(s));
            }
            return res;
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> scaleFactorConf(String scaleFactorXml, String scaleFactorId) {
        Map<String, String> conf = new HashMap<>();
        ScaleFactors scaleFactors = ScaleFactors.INSTANCE;
        scaleFactors.initialize(scaleFactorXml); // use default if empty
        if (!scaleFactors.value.containsKey(scaleFactorId)) {
            throw new IllegalArgumentException("Scale factor " + scaleFactorId + " does not exist");
        }
        ScaleFactor scaleFactor = scaleFactors.value.get(scaleFactorId);
        System.out.println("Applied configuration from " + (scaleFactorXml.isEmpty() ? "default" : scaleFactorXml)
                               + " of scale factor " + scaleFactorId);
        for (Map.Entry<String, String> e : scaleFactor.properties.entrySet()) {
            conf.put(e.getKey(), e.getValue());
        }
        return conf;
    }


}
