package org.matsim.actitopp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Population;
import org.matsim.api.core.v01.population.PopulationWriter;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.core.utils.gis.ShapeFileReader;
import org.matsim.facilities.ActivityFacilities;
import org.matsim.facilities.ActivityFacility;
import org.matsim.facilities.FacilitiesWriter;
import org.matsim.facilities.Facility;
import org.matsim.households.Household;
import org.matsim.households.Households;
import org.matsim.households.HouseholdsWriterV10;
import org.matsim.utils.objectattributes.attributable.Attributes;
import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author dziemke
 */
public class IvtPopulationParser {
    private static final Logger LOG = Logger.getLogger(IvtPopulationParser.class);

    // Input file column headers
    private static final String PERSON_ID = "person_id";
    private static final String HOUSEHOLD_ID = "household_id";
    private static final String SEX = "sex";
    private static final String AGE = "age";
    private static final String HOME_X = "home_x";
    private static final String HOME_Y = "home_y";
    private static final String HOUSEHOLD_SIZE = "household_size";
    // private static final String HOME_ZONE_ID = "home_zone_id";
    private static final String MUNICIPALITY_TYPE = "municipality_type";
    private static final String HOME_MUNICIPALITY_ID = "home_municipality_id";
    private static final String CANTON_ID = "canton_id";
    private static final String NUMBER_OF_CARS_CLASS = "number_of_cars_class";
    // private static final String DRIVING_LICENSE = "driving_license";
    // private static final String CAR_AVAILABILITY = "car_availability";
    private static final String EMPLOYED = "employed";


    public static void main(String[] args) {
        // TODO try to use gzipped file
        Path inputFile = Paths.get("../../svn/shared-svn/projects/snf-big-data/data/original_files/ivt_syn_pop/population.csv");
        String outputFileRoot = "../../svn/shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/";
        double sampleSize = 0.01;
        List<Integer> cantonsIncluded = Arrays.asList(24); // 24 = Neuenburg

        Scenario scenario = IvtPopulationParser.createScenarioFromIvtInput(inputFile, cantonsIncluded);
        Scenario sampleScenario = IvtPopulationParser.createScenarioSample(sampleSize, scenario);

        // The following makes sense if the scenario to work with has a different spatial resolution than the population input file.
        // Use cautiously as it may lead to inaccuracies resulting from inaccurate shapefiles
        // String municipalitiesShapeFile = "../../svn/shared-svn/projects/snf-big-data/data/original_files/municipalities/2012_boundaries/G3G12_EPSG-2056.shp";
        // IvtPopulationParser.assignMunicipalityIdsBasedOnShapefile(sampleScenario, municipalitiesShapeFile);

        int sampleSize_pct = (int) (sampleSize * 100);
        String suffix = "_" + sampleSize_pct + "pct";
        IvtPopulationParser.writeMatsimFiles(sampleScenario, outputFileRoot, suffix);
    }

    public static Scenario createScenarioFromIvtInput(Path inputFile, List<Integer> cantonsIncluded) {
        LOG.info("Start creating population, households, and facilities.");
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Population population = scenario.getPopulation();
        Households households = scenario.getHouseholds();

        ActivityFacilities facilities = scenario.getActivityFacilities();

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(',').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                int cantonId = (int) Double.parseDouble(record.get(CANTON_ID));
                if (cantonsIncluded != null && !cantonsIncluded.contains(cantonId)) {
                    continue;
                }

                Id<Person> personId = Id.createPersonId(record.get(PERSON_ID));
                Person person = population.getFactory().createPerson(personId);
                population.addPerson(person);

                Household household = null;
                Id<Household> householdId = Id.create(record.get(HOUSEHOLD_ID), Household.class);
                household = addOrGetHousehold(households, householdId);
                household.getMemberIds().add(personId);

                person.getAttributes().putAttribute(AttributeLabels.household_id.toString(), householdId.toString());

                int householdSize = Integer.valueOf(record.get(HOUSEHOLD_SIZE));
                assignOrCheckHouseholdSize(household, householdSize);

                Integer sex = Integer.valueOf(record.get(SEX));
                person.getAttributes().putAttribute(AttributeLabels.gender.toString(), getGender(sex).toString());

                Integer age = Integer.valueOf(record.get(AGE));
                person.getAttributes().putAttribute(AttributeLabels.age.toString(), age);

                ActivityFacility facility = null;
                Coord homeCoord = CoordUtils.createCoord(Double.valueOf(record.get(HOME_X)), Double.valueOf(record.get(HOME_Y)));
                facility = addOrGetFacility(facilities, householdId, homeCoord);
                facility.getAttributes().putAttribute(AttributeLabels.household_id.toString(), householdId.toString());
                person.getAttributes().putAttribute(AttributeLabels.facility_id.toString(), facility.getId().toString());
                household.getAttributes().putAttribute(AttributeLabels.facility_id.toString(), facility.getId().toString());

                String municipalityTypeRecord = record.get(MUNICIPALITY_TYPE); // urban, suburban, rural
                MunicipalityType municipalityType = getMunicipalityType(municipalityTypeRecord);
                household.getAttributes().putAttribute(AttributeLabels.municipality_type.toString(), municipalityType.toString());
                person.getAttributes().putAttribute(AttributeLabels.municipality_type.toString(), municipalityType.toString());

                int municipalityId = (int) Double.parseDouble(record.get(HOME_MUNICIPALITY_ID));
                person.getAttributes().putAttribute(AttributeLabels.municipality_id.toString(), municipalityId);
                household.getAttributes().putAttribute(AttributeLabels.municipality_id.toString(), municipalityId);
                facility.getAttributes().putAttribute(AttributeLabels.municipality_id.toString(), municipalityId);

                person.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);
                household.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);
                facility.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);

                Integer numberOfCars = Integer.valueOf(record.get(NUMBER_OF_CARS_CLASS));
                // Note: The classes are 0, 1, 2, 3+, but we simplify it to 0, 1, 2, 3.
                household.getAttributes().putAttribute(AttributeLabels.number_of_cars.toString(), numberOfCars);
                person.getAttributes().putAttribute(AttributeLabels.number_of_cars.toString(), numberOfCars);

                boolean employed = Boolean.parseBoolean(record.get(EMPLOYED));
                person.getAttributes().putAttribute(AttributeLabels.employed.toString(), employed);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        assignNumberOfChildrenToPopulationAndHouseholds(households, population);

        return scenario;
    }

    /*
    Use cautiously as it may lead to inaccuracies resulting from inaccurate shapefiles
     */
    public static void assignMunicipalityIdsBasedOnShapefile(Scenario scenario, String municipalitiesShapeFile) {
        LOG.info("Start checking if locations fit.");
        Map<Integer, Geometry> municipalityGeometries = new HashMap<>();
        GeometryFactory gf = new GeometryFactory();

        ShapeFileReader sfr = new ShapeFileReader();
        for (SimpleFeature sf : sfr.getAllFeatures(municipalitiesShapeFile)) {
            // Attributes: GMDE, BEZIRK, KT, NAME
            int municapalityId = Integer.valueOf(sf.getAttribute("GMDE").toString());
            Geometry geometry = (Geometry) sf.getDefaultGeometry();
            municipalityGeometries.put(municapalityId, geometry);
        }

        for (Household household : scenario.getHouseholds().getHouseholds().values()) {
            Attributes householdAttributes = household.getAttributes();
            Id<ActivityFacility> facilityId = Id.create((String) householdAttributes.getAttribute(AttributeLabels.facility_id.toString()), ActivityFacility.class);

            Facility facility = scenario.getActivityFacilities().getFacilities().get(facilityId);
            Point householdLocation = gf.createPoint(new Coordinate(facility.getCoord().getX(), facility.getCoord().getY()));

            int municipalityId = (int) householdAttributes.getAttribute(AttributeLabels.municipality_id.toString());
            Geometry munincipalityGeometry = municipalityGeometries.get(municipalityId);

            if (munincipalityGeometry != null) {
                if (munincipalityGeometry.contains(householdLocation)) {
                    // LOG.info("Household location contained in corresponding municipality " + municipalityId + ". Municipality is kept.");
                    householdAttributes.putAttribute(AttributeLabels.municipality_id.toString(), municipalityId);
                } else {
                    LOG.info("Municipality " + municipalityId + " is in shapefile, but does not contain household location. Therefore, change municipality.");
                    updateMunicipalityId(municipalityGeometries, householdAttributes, householdLocation, municipalityId);
                }
            } else {
                LOG.info("Municipality " + municipalityId + " is not in shapefile. Therefore, change municipality.");
                updateMunicipalityId(municipalityGeometries, householdAttributes, householdLocation, municipalityId);
            }
        }
    }

    public static Scenario createScenarioSample(double sampleSize, Scenario scenario) {
        LOG.info("Start sampling population, households, and facilities.");
        Scenario scenarioSample = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Population sampledPopulation = scenarioSample.getPopulation();
        Households sampledHouseholds = scenarioSample.getHouseholds();
        ActivityFacilities sampledFacilities = scenarioSample.getActivityFacilities();

        for (Household household : scenario.getHouseholds().getHouseholds().values()) {
            Random random = MatsimRandom.getLocalInstance();
            if (random.nextDouble() < sampleSize) {
                sampledHouseholds.getHouseholds().put(household.getId(), household);
                for (Id<Person> personId : household.getMemberIds()) {
                    sampledPopulation.addPerson(scenario.getPopulation().getPersons().get(personId));
                }
                Id<Facility> facilityId = Id.create(household.getAttributes().getAttribute(AttributeLabels.facility_id.toString()).toString(), Facility.class);
                ActivityFacility facility = scenario.getActivityFacilities().getFacilities().get(facilityId);
                sampledFacilities.addActivityFacility(facility);
            }
        }
        return scenarioSample;
    }

    public static void writeMatsimFiles(Scenario scenario, String outputFileRoot, String suffix) {
        (new File(outputFileRoot)).mkdir();
        LOG.info("Start writing population, households, and facilities files.");
        PopulationWriter popWriter = new PopulationWriter(scenario.getPopulation());
        popWriter.write(outputFileRoot + "population" + suffix + ".xml.gz");

        HouseholdsWriterV10 householdsWriter = new HouseholdsWriterV10(scenario.getHouseholds());
        householdsWriter.writeFile(outputFileRoot + "households" + suffix + ".xml.gz");

        FacilitiesWriter facilitiesWriter = new FacilitiesWriter(scenario.getActivityFacilities());
        facilitiesWriter.write(outputFileRoot + "facilities" + suffix + ".xml.gz");
    }

    // private methods ----------------------------------------

    private static Household addOrGetHousehold(Households households, Id<Household> householdId) {
        Household household;
        if (!households.getHouseholds().containsKey(householdId)) {
            household = households.getFactory().createHousehold(householdId);
            households.getHouseholds().put(household.getId(), household);
        } else {
            household = households.getHouseholds().get(householdId);
        }
        return household;
    }

    private static ActivityFacility addOrGetFacility(ActivityFacilities facilities, Id<Household> householdId, Coord coord) {
        ActivityFacility facility = null;
        Id<ActivityFacility> facilityId = Id.create(householdId, ActivityFacility.class);
        if (!facilities.getFacilities().containsKey(facilityId)) {
            facility = facilities.getFactory().createActivityFacility(facilityId, coord);
            facility.getAttributes().putAttribute(AttributeLabels.household_id.toString(), householdId);
            facilities.addActivityFacility(facility);
        } else {
            facility = facilities.getFacilities().get(facilityId);
        }
        return facility;
    }

    private static void assignOrCheckHouseholdSize(Household household, int householdSize) {
        int householdSizeAlreadyStored;
        if (household.getAttributes().getAttribute(HOUSEHOLD_SIZE) == null) {
            householdSizeAlreadyStored = 0;
        } else {
            householdSizeAlreadyStored = (int) household.getAttributes().getAttribute(HOUSEHOLD_SIZE);
        }
        ;
        if (householdSizeAlreadyStored == 0) {
            household.getAttributes().putAttribute(AttributeLabels.household_size.toString(), householdSize);
        } else {
            if (householdSizeAlreadyStored != householdSize) {
                throw new IllegalArgumentException("Two people of the same household must not have different household size values.");
            }
        }
    }

    private static Gender getGender(Integer sex) {
        Gender gender;
        if (sex == 1) {
            gender = Gender.male;
        } else if (sex == 2) {
            gender = Gender.female;
        } else {
            gender = Gender.unspecified;
        }
        return gender;
    }

    private static MunicipalityType getMunicipalityType(String municipalityTypeRecord) {
        MunicipalityType municipalityType = null;
        if (municipalityTypeRecord.equals("urban")) {
            municipalityType = MunicipalityType.urban;
        } else if (municipalityTypeRecord.equals("suburban")) {
            municipalityType = MunicipalityType.suburban;
        } else if (municipalityTypeRecord.equals("rural")) {
            municipalityType = MunicipalityType.rural;
        } else {
            throw new IllegalArgumentException("Municipality type should not be other than urban, suburban, or rural. It is " + municipalityType);
        }
        return municipalityType;
    }

    private static void assignNumberOfChildrenToPopulationAndHouseholds(Households households, Population population) {
        LOG.info("Start assigning number of children to population and households.");
        for (Household household : households.getHouseholds().values()) {
            boolean children_0_10 = false;
            boolean children_0_18 = false;
            for (Id<Person> personId : household.getMemberIds()) {
                Person person = population.getPersons().get(personId);
                int age = (int) person.getAttributes().getAttribute(AttributeLabels.age.toString());
                // TODO ask Tim: Fällt ein Kind das bspw. 8 Jahre alt ist nur in die Kategorie 0-10 oder auch in die Kategorie <18?
                if (age <= 10) {
                    children_0_10 = true;
                }
                if (age <= 18) {
                    children_0_18 = true;
                }
            }

            household.getAttributes().putAttribute(AttributeLabels.children_0_10.toString(), children_0_10);
            household.getAttributes().putAttribute(AttributeLabels.children_0_18.toString(), children_0_18);

            for (Id<Person> personId : household.getMemberIds()) {
                Person person = population.getPersons().get(personId);
                person.getAttributes().putAttribute(AttributeLabels.children_0_10.toString(), children_0_10);
                person.getAttributes().putAttribute(AttributeLabels.children_0_18.toString(), children_0_18);
            }
        }
    }

    private static void updateMunicipalityId(Map<Integer, Geometry> municipalityGeometries, Attributes attr, Point householdLocation, int municipalityId) {
        boolean updated = false;
        for (int municipalityGeometryId : municipalityGeometries.keySet()) {
            Geometry municipalityGeometry = municipalityGeometries.get(municipalityGeometryId);
            if (municipalityGeometry.contains(householdLocation)) {
                attr.putAttribute(AttributeLabels.municipality_id.toString(), municipalityGeometryId);
                LOG.info("Updated municipality ID from " + municipalityId + " to " + municipalityGeometryId + ".");
                updated = true;
                break;
            }
        }
        if (updated == false) {
            LOG.warn("The municipality which was " + municipalityId + " before could not be updated. It is, therefore, kept.");
        }
    }

    enum AttributeLabels {
        household_id, gender, age, household_size, municipality_type, canton_id, municipality_id, number_of_cars,
        employed, children_0_10, children_0_18, facility_id
    }

    enum Gender {female, male, unspecified}

    enum MunicipalityType {urban, suburban, rural}
}