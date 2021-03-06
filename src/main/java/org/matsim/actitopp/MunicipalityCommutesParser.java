package org.matsim.actitopp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.utils.gis.ShapeFileReader;
import org.matsim.counts.Count;
import org.matsim.counts.Counts;
import org.matsim.counts.CountsWriter;
import org.matsim.counts.Volume;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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

    private List<Id<ActiToppUtils.Municipality>> consideredMunicipalities = new ArrayList<>();
    private Map<String, Id<ActiToppUtils.Municipality>> municipalityNameToIdMap = new HashMap<>();
    private Map<Id<ActiToppUtils.Municipality>, Id<ActiToppUtils.Canton>> municipalityToCantonMap = new HashMap<>();
    private Map<Id<ActiToppUtils.Municipality>, Id<ActiToppUtils.Municipality>> municipalityIdConversionMap;
    private Map<String, String> municipalityNameConversionMap;

    public MunicipalityCommutesParser(Path inputFileMunicipalities) {
        readAndStoreMunicipalities(inputFileMunicipalities);
    }

    public static void main(String[] args) {
        // TODO try to use gzipped file
        Path inputFileMunicipalities = Paths.get("../../shared-svn/projects/snf-big-data/data/original_files/municipalities/2012/12501_131.csv"); // 2012
        Path inputFileMatrix = Paths.get("../../shared-svn/projects/snf-big-data/data/original_files/swisscom/eth2/Eth4_20161001.txt");

        // List<Id<ActiToppUtils.Canton>> cantonsIncluded = Arrays.asList(Id.create(24, Canton.class)); // 24 = Neuenburg
        List<Id<ActiToppUtils.Canton>> cantonsIncluded = null; // null = all Switzerland

        String shapeFileWithToBeIncludedMunicipalities = "../../shared-svn/projects/snf-big-data/data/scenario/zh-metro_10pct/2018_boundaries/g2g18_zh-metro.shp";

        Path inputFileMunicipalityUpdates = Paths.get("../../shared-svn/projects/snf-big-data/data/original_files/municipalities/2012-2018_changes/2012-2018_changes.csv");
        String idLabelOld = "Regions-ID_Alt";
        String idLabelNew = "Regions-ID_Neu";
        String nameLabelOld = "Regionsname_Alt";
        String nameLabelNew = "Regionsname_Neu";

        double sampleSize = 0.01;
        // double sampleSize = 0.10;

        // String outputFileCommuteCounts = "../../shared-svn/projects/snf-big-data/data/commute_counts/20161001_neuenburg_2018_1pct.xml.gz";
        // String outputFileCommuteCounts = "../../shared-svn/projects/snf-big-data/data/commute_counts/20161001_switzerland_2018_1pct.xml.gz"; // recreate it
        String outputFileCommuteCounts = "../../shared-svn/projects/snf-big-data/data/commute_counts/20161001_zh-metro_2018_1pct.xml.gz"; // recreate it

        MunicipalityCommutesParser commuteMatrixParser = new MunicipalityCommutesParser(inputFileMunicipalities);
        // commuteMatrixParser.setCantonsIncluded(cantonsIncluded);
        commuteMatrixParser.setMunicipalitiesIncludedByShpFile(shapeFileWithToBeIncludedMunicipalities, "GMDNR");
        // Need to be added manually as they are not contained in the shapefile, c.f. 2012-2018_changes.csv
        // 132/133 -> 295; 171/179 -> 297; 174/175 -> 296; 212 -> 298; 217/222 -> 294; 229 -> 298; 4069 -> 4063
        commuteMatrixParser.addIncludedMunicipalities(Arrays.asList(132, 133, 171, 174, 175, 179, 212, 217, 222, 229, 4069));

        commuteMatrixParser.setMunicipalityUpdater(inputFileMunicipalityUpdates, idLabelOld, idLabelNew, nameLabelOld, nameLabelNew);
        Counts commuteCounts = commuteMatrixParser.createCommuteCounts(inputFileMatrix, "commuteCounts", 20161001);
        commuteMatrixParser.scaleCounts(commuteCounts, sampleSize);
        commuteMatrixParser.writeCommuteCounts(commuteCounts, outputFileCommuteCounts);
    }

    public static void writeCommuteCounts(Counts commuteCounts, String outputFile) {
        LOG.info("Start writing commute counts file.");
        CountsWriter countsWriter = new CountsWriter(commuteCounts);
        countsWriter.write(outputFile);
    }

    private void readAndStoreMunicipalities(Path inputFile) {
        LOG.info("Start creating municipalities map.");

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(';').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                Id<ActiToppUtils.Municipality> munId = Id.create(Integer.valueOf(record.get(GDENR)), ActiToppUtils.Municipality.class);
                String name = record.get(GDENAME);
                String canton = record.get(KANTON);
                String[] splitString1 = canton.split("\\(");
                String[] splitString2 = splitString1[1].split("\\)");
                Id<ActiToppUtils.Canton> cantonId = Id.create(Integer.valueOf(splitString2[0]), ActiToppUtils.Canton.class);
                LOG.info("Municipality " + name + " with id " + munId + " added (Canton: " + cantonId.toString() + ").");

                municipalityNameToIdMap.put(name, munId);
                municipalityToCantonMap.put(munId, cantonId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Counts createCommuteCounts(Path inputFile, String name, int dateTag) {
        LOG.info("Start creating commute counts.");
        Counts commuteCounts = new Counts();
        commuteCounts.setName(name);
        commuteCounts.setYear(dateTag);

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(',').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                int hour = Integer.valueOf(record.get(HOUR)) + 1;
                String muniFromName = record.get(MUNI_FROM);
                String muniToName = record.get(MUNI_TO);
                int value = Integer.valueOf(record.get(COUNT));

                Id<ActiToppUtils.Municipality> muniFromId = municipalityNameToIdMap.get(muniFromName);
                Id<ActiToppUtils.Municipality> muniToId = municipalityNameToIdMap.get(muniToName);

                if (consideredMunicipalities.size() != 0) {
                    if (!consideredMunicipalities.contains(muniFromId) || !consideredMunicipalities.contains(muniToId)) {
                        continue;
                    }
                }

                if (municipalityIdConversionMap != null) {
                    muniFromId = municipalityIdConversionMap.get(muniFromId);
                    muniToId = municipalityIdConversionMap.get(muniToId);
                    muniFromName = municipalityNameConversionMap.get(muniFromName);
                    muniToName = municipalityNameConversionMap.get(muniToName);
                }

                String realtionIds = muniFromId + "_" + muniToId;
                String description = muniFromName + "_" + muniToName;
                Id<ActiToppUtils.Relation> id = Id.create(realtionIds, ActiToppUtils.Relation.class);

                Count count;
                if (!commuteCounts.getCounts().containsKey(id)) {
                    count = commuteCounts.createAndAddCount(id, description);
                } else {
                    count = commuteCounts.getCount(id);
                }

                if (count.getVolume(hour) == null) {
                    count.createVolume(hour, value);
                } else {
                    double oldValue = ((Volume) count.getVolumes().get(hour)).getValue();
                    ((Volume) count.getVolumes().get(hour)).setValue(oldValue + value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return commuteCounts;
    }

    public void setMunicipalityUpdater(Path inputMunicipalityConversion, String idLabelOld, String idLabelNew, String nameLabelOld, String nameLabelNew) {
        LOG.info("Start creating municipalities map.");
        this.municipalityIdConversionMap = new HashMap<>();
        this.municipalityNameConversionMap = new HashMap<>();

        try (CSVParser parser = CSVParser.parse(inputMunicipalityConversion, StandardCharsets.UTF_8, CSVFormat.newFormat(';').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                Id<ActiToppUtils.Municipality> munIdOld = Id.create(Integer.valueOf(record.get(idLabelOld)), ActiToppUtils.Municipality.class);
                Id<ActiToppUtils.Municipality> munIdNew = Id.create(Integer.valueOf(record.get(idLabelNew)), ActiToppUtils.Municipality.class);
                String munNameOld = record.get(nameLabelOld);
                String munNameNew = record.get(nameLabelNew);

                municipalityIdConversionMap.put(munIdOld, munIdNew);
                municipalityNameConversionMap.put(munNameOld, munNameNew);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void scaleCounts(Counts commuteCounts, double sampleSize) {
        Random random = MatsimRandom.getLocalInstance();
        for (Object count : commuteCounts.getCounts().values()) {
            for (Object volume : ((Count) count).getVolumes().values()) {
                double value = ((Volume) volume).getValue();
                double scaledValue = value * sampleSize;
                int intPart = (int) scaledValue;
                double decimalPart = scaledValue - intPart;
                double result;
                if (random.nextDouble() < decimalPart) {
                    result = Math.ceil(scaledValue);
                } else {
                    result = Math.floor(scaledValue);
                }
                ((Volume) volume).setValue(result);
            }
        }
    }

    public void setCantonsIncluded(List<Id<ActiToppUtils.Canton>> cantonsIncluded) {
        for (Id<ActiToppUtils.Municipality> municipalityId : municipalityToCantonMap.keySet()) {
            if (cantonsIncluded != null && cantonsIncluded.contains(municipalityToCantonMap.get(municipalityId))) {
                consideredMunicipalities.add(municipalityId);
            }
        }
    }

    public void setMunicipalitiesIncludedByShpFile(String shapeFile, String municipalityIdentifier) {
        ShapeFileReader shapeFileReader = new ShapeFileReader();
        Collection<SimpleFeature> features = shapeFileReader.readFileAndInitialize(shapeFile);

        for (SimpleFeature feature : features) {
            Id<ActiToppUtils.Municipality> munId = Id.create(String.valueOf(feature.getAttribute(municipalityIdentifier)), ActiToppUtils.Municipality.class);
            consideredMunicipalities.add(munId);
        }
    }

    public void addIncludedMunicipalities(List<Integer> municipalityIds) {
        for (Integer municipalityId : municipalityIds) {
            Id<ActiToppUtils.Municipality> munId = Id.create(municipalityId, ActiToppUtils.Municipality.class);
            consideredMunicipalities.add(munId);
        }
    }
}