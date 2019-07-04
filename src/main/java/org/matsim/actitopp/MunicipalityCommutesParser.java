package org.matsim.actitopp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.counts.Count;
import org.matsim.counts.Counts;
import org.matsim.counts.CountsWriter;
import org.matsim.utils.objectattributes.ObjectAttributes;
import org.matsim.utils.objectattributes.ObjectAttributesXmlWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dziemke
 */
public class MunicipalityCommutesParser {
    private static final Logger LOG = Logger.getLogger(MunicipalityCommutesParser.class);

    // Input file column headers
    // Regions-ID;Regionsname;Kantonszugehörigkeit
    private static final String GDENR = "Regions-ID";
    private static final String GDENAME = "Regionsname";
    private static final String KANTON = "Kantonszugehörigkeit";

    // hour,muni_from,muni_to,count
    private static final String HOUR = "hour";
    private static final String MUNI_FROM = "muni_from";
    private static final String MUNI_TO = "muni_to";
    private static final String COUNT = "count";
    ObjectAttributes municipalities = new ObjectAttributes();
    private Map<String, Integer> municipalityNameToIdMap = new HashMap<>();
    private Counts commuteCounts = new Counts();

    public static void main(String[] args) {
        // TODO try to use gzipped file
        Path inputFileMunicipalities = Paths.get("../../svn/shared-svn/projects/snf-big-data/data/original_files/municipalities/2012/12501_131.csv");
        Path inputFileMatrix = Paths.get("../../svn/shared-svn/projects/snf-big-data/data/original_files/swisscom/eth2/Eth4_20161001.txt");
        String outputFileMunicipalities = "../../svn/shared-svn/projects/snf-big-data/data/commute_counts/municipalities_2012.xml.gz";
        String outputFileCommuteCounts = "../../svn/shared-svn/projects/snf-big-data/data/commute_counts/20161001.xml.gz";

        MunicipalityCommutesParser commuteMatrixParser = new MunicipalityCommutesParser();
        commuteMatrixParser.readAndStoreMunicipalities(inputFileMunicipalities);
        commuteMatrixParser.createCommuteCounts(inputFileMatrix);
        commuteMatrixParser.writeMunicipalitiesFile(outputFileMunicipalities);
        commuteMatrixParser.writeCommuteCounts(outputFileCommuteCounts);
    }

    private void readAndStoreMunicipalities(Path inputFile) {
        LOG.info("Start creating municipalities map.");

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(';').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                Integer id = Integer.valueOf(record.get(GDENR));
                String name = record.get(GDENAME);
                String canton = record.get(KANTON);
                String[] splitString1 = canton.split("\\(");
                String[] splitString2 = splitString1[1].split("\\)");
                int cantonId = Integer.valueOf(splitString2[0]);
                LOG.info("Municipality " + name + " with id " + id + " added (Canton: " + cantonId + ").");

                municipalityNameToIdMap.put(name, id);

                municipalities.putAttribute(id.toString(), "name", name);
                municipalities.putAttribute(id.toString(), "cantonId", cantonId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createCommuteCounts(Path inputFile) {
        LOG.info("Start creating commute counts.");

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(',').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                int hour = Integer.valueOf(record.get(HOUR));
                String muniFrom = record.get(MUNI_FROM);
                String muniTo = record.get(MUNI_TO);
                int muniFromId = municipalityNameToIdMap.get(muniFrom);
                int muniToId = municipalityNameToIdMap.get(muniTo);
                int value = Integer.valueOf(record.get(COUNT));

                String realtionIds = muniFromId + "_" + muniToId;
                String description = muniFrom + "_" + muniTo;
                Id<Relation> id = Id.create(realtionIds, Relation.class);

                Count count;
                if (!commuteCounts.getCounts().containsKey(id)) {
                    count = commuteCounts.createAndAddCount(id, description);
                } else {
                    count = commuteCounts.getCount(id);
                }
                count.createVolume(hour + 1, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeMunicipalitiesFile(String outputFile) {
        LOG.info("Start writing municipalities file.");
        ObjectAttributesXmlWriter writer = new ObjectAttributesXmlWriter(municipalities);
        writer.writeFile(outputFile);
    }

    private void writeCommuteCounts(String outputFile) {
        LOG.info("Start writing commute counts file.");
        CountsWriter countsWriter = new CountsWriter(commuteCounts);
        countsWriter.write(outputFile);
    }

    // Inner class
    class Relation {
    }
}