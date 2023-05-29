package ldbc.finbench.datagen.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ScaleFactors {
    public TreeMap<String, ScaleFactor> value;

    public static final ScaleFactors INSTANCE = new ScaleFactors();

    private ScaleFactors() {
    }

    public void initialize(String scaleFactorsXml) {
        try {
            value = new TreeMap<>();
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream configFile = scaleFactorsXml.isEmpty()
                ? ScaleFactors.class.getResourceAsStream("/scale_factors.xml")
                : Files.newInputStream(Paths.get(scaleFactorsXml));
            Document doc = builder.parse(configFile);
            doc.getDocumentElement().normalize();

            System.out.println("Reading scale factors from " + (scaleFactorsXml.isEmpty() ? "default" :
                scaleFactorsXml) + "...");
            NodeList nodes = doc.getElementsByTagName("scale_factor");
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String scaleFactorName = element.getAttribute("name");
                    ScaleFactor scaleFactor = new ScaleFactor();
                    NodeList properties = ((Element) node).getElementsByTagName("property");
                    for (int j = 0; j < properties.getLength(); ++j) {
                        Element property = (Element) properties.item(j);
                        String name = property.getElementsByTagName("name").item(0).getTextContent();
                        String value = property.getElementsByTagName("value").item(0).getTextContent();
                        scaleFactor.properties.put(name, value);
                    }
                    System.out.println("Available scale factor configuration set " + scaleFactorName);
                    value.put(scaleFactorName, scaleFactor);
                }
            }
            System.out.println("Number of scale factors read " + value.size());
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
