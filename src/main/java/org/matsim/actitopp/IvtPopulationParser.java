package org.matsim.actitopp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.Population;
import org.matsim.api.core.v01.population.PopulationFactory;
import org.matsim.api.core.v01.population.PopulationWriter;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.facilities.*;
import org.matsim.households.Household;
import org.matsim.households.Households;
import org.matsim.households.HouseholdsFactory;
import org.matsim.households.HouseholdsWriterV10;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
    // private static final String HOME_MUNICIPALITY_ID = "home_municipality_id";
    private static final String CANTON_ID = "canton_id";
    private static final String NUMBER_OF_CARS_CLASS = "number_of_cars_class";
    // private static final String DRIVING_LICENSE = "driving_license";
    // private static final String CAR_AVAILABILITY = "car_availability";
    private static final String EMPLOYED = "employed";

    private Population population;
    private Households households;
    private ActivityFacilities facilities;

    private List<Integer> cantonsIncluded;


    public static void main(String[] args) {
        // TODO try to use gzipped file
        Path inputFile = Paths.get("../../svn/shared-svn/projects/snf-big-data/data/original_files/ivt_syn_pop/population.csv");
        String outputFileRoot = "../../svn/shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/";
        double sampleSize = 0.01;
        List<Integer> cantonsIncluded = Arrays.asList(24); // 24 = Neuenburg

        IvtPopulationParser ivtPopulationParser = new IvtPopulationParser();
        ivtPopulationParser.setCantonsIncluded(cantonsIncluded);
        ivtPopulationParser.createPopulationHouseholdsAndFacilities(inputFile);
        // ivtPopulationParser.writeMatsimFiles(outputFileRoot);

        ivtPopulationParser.samplePopulationHouseholdsAndFacilitesAndWrite(sampleSize, outputFileRoot);
    }

    private void createPopulationHouseholdsAndFacilities(Path inputFile) {
        LOG.info("Start creating population, households, and facilities.");
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        population = scenario.getPopulation();
        households = scenario.getHouseholds();
        facilities = scenario.getActivityFacilities();

        PopulationFactory populationFactory = population.getFactory();
        HouseholdsFactory householdsFactory = households.getFactory();
        ActivityFacilitiesFactory activityFacilitiesFactory = facilities.getFactory();

        try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, CSVFormat.newFormat(',').withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                int cantonId = (int) Double.parseDouble(record.get(CANTON_ID));
                if (cantonsIncluded != null && !cantonsIncluded.contains(cantonId)) {
                    continue;
                }

                Id<Person> personId = Id.createPersonId(record.get(PERSON_ID));
                Person person = populationFactory.createPerson(personId);
                population.addPerson(person);

                Household household = null;
                Id<Household> householdId = Id.create(record.get(HOUSEHOLD_ID), Household.class);
                household = addOrGetHousehold(householdsFactory, householdId);
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
                facility = addOrGetFacility(activityFacilitiesFactory, householdId, homeCoord);
                facility.getAttributes().putAttribute(AttributeLabels.household_id.toString(), householdId.toString());
                household.getAttributes().putAttribute(AttributeLabels.facility_id.toString(), facility.getId().toString());

                String municipalityTypeRecord = record.get(MUNICIPALITY_TYPE); // urban, suburban, rural
                MunicipalityType municipalityType = getMunicipalityType(municipalityTypeRecord);
                household.getAttributes().putAttribute(AttributeLabels.municipality_type.toString(), municipalityType.toString());
                person.getAttributes().putAttribute(AttributeLabels.municipality_type.toString(), municipalityType.toString());

                person.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);
                household.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);
                facility.getAttributes().putAttribute(AttributeLabels.canton_id.toString(), cantonId);

                Integer numberOfCars = Integer.valueOf(record.get(NUMBER_OF_CARS_CLASS));
                // Note: The classes are 0, 1, 2, 3+, but we simplify it to 0, 1, 2, 3.
                household.getAttributes().putAttribute(AttributeLabels.number_of_cars.toString(), numberOfCars);
                person.getAttributes().putAttribute(AttributeLabels.number_of_cars.toString(), numberOfCars);

                // boolean drivingLicense = Boolean.parseBoolean(record.get(DRIVING_LICENSE));
                // person.getAttributes().putAttribute(AttributeLabels.driving_license.toString(), drivingLicense);

                // int carAvailability = Integer.parseInt(record.get(CAR_AVAILABILITY));
                // person.getAttributes().putAttribute(AttributeLabels.car_availability.toString(), carAvailability);

                boolean employed = Boolean.parseBoolean(record.get(EMPLOYED));
                person.getAttributes().putAttribute(AttributeLabels.employed.toString(), employed);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        assignNumberOfChildrenToPopulationAndHouseholds();
    }

    private Household addOrGetHousehold(HouseholdsFactory householdsFactory, Id<Household> householdId) {
        Household household;
        if (!households.getHouseholds().containsKey(householdId)) {
            household = householdsFactory.createHousehold(householdId);
            households.getHouseholds().put(household.getId(), household);
        } else {
            household = households.getHouseholds().get(householdId);
        }
        return household;
    }

    private ActivityFacility addOrGetFacility(ActivityFacilitiesFactory facilitiesFactory, Id<Household> householdId, Coord coord) {
        ActivityFacility facility = null;
        Id<ActivityFacility> facilityId = Id.create(householdId, ActivityFacility.class);
        if (!facilities.getFacilities().containsKey(facilityId)) {
            facility = facilitiesFactory.createActivityFacility(facilityId, coord);
            facility.getAttributes().putAttribute(AttributeLabels.household_id.toString(), householdId);
            facilities.addActivityFacility(facility);
        } else {
            facility = facilities.getFacilities().get(facilityId);
        }
        return facility;
    }

    private void assignOrCheckHouseholdSize(Household household, int householdSize) {
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

    private Gender getGender(Integer sex) {
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

    private MunicipalityType getMunicipalityType(String municipalityTypeRecord) {
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

    private void assignNumberOfChildrenToPopulationAndHouseholds() {
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

    private void samplePopulationHouseholdsAndFacilitesAndWrite(double sampleSize, String outputFileRoot) {
        LOG.info("Start sampling population, households, and facilities.");
        Scenario scenarioSample = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        Population sampledPopulation = scenarioSample.getPopulation();
        Households sampledHouseholds = scenarioSample.getHouseholds();
        ActivityFacilities sampledFacilities = scenarioSample.getActivityFacilities();

        for (Household household : households.getHouseholds().values()) {
            Random random = MatsimRandom.getLocalInstance();
            if (random.nextDouble() < 0.01) {
                sampledHouseholds.getHouseholds().put(household.getId(), household);
                for (Id<Person> personId : household.getMemberIds()) {
                    sampledPopulation.addPerson(population.getPersons().get(personId));
                }
                Id<Facility> facilityId = Id.create(household.getAttributes().getAttribute(AttributeLabels.facility_id.toString()).toString(), Facility.class);
                ActivityFacility facility = facilities.getFacilities().get(facilityId);
                sampledFacilities.addActivityFacility(facility);
            }
        }

        int sampleSize_pct = (int) (sampleSize * 100);
        String suffix = "_" + sampleSize_pct + "pct";

        LOG.info("Start writing population, households, and facilities files.");
        PopulationWriter popWriter = new PopulationWriter(sampledPopulation);
        popWriter.write(outputFileRoot + "population" + suffix + ".xml.gz");

        HouseholdsWriterV10 householdsWriter = new HouseholdsWriterV10(sampledHouseholds);
        householdsWriter.writeFile(outputFileRoot + "households" + suffix + ".xml.gz");

        FacilitiesWriter facilitiesWriter = new FacilitiesWriter(sampledFacilities);
        facilitiesWriter.write(outputFileRoot + "facilities" + suffix + ".xml.gz");
    }

    private void writeMatsimFiles(String outputFileRoot) {
        LOG.info("Start writing population, households, and facilities files.");
        PopulationWriter popWriter = new PopulationWriter(population);
        popWriter.write(outputFileRoot + "population.xml.gz");

        HouseholdsWriterV10 householdsWriter = new HouseholdsWriterV10(households);
        householdsWriter.writeFile(outputFileRoot + "households.xml.gz");

        FacilitiesWriter facilitiesWriter = new FacilitiesWriter(facilities);
        facilitiesWriter.write(outputFileRoot + "facilities.xml.gz");
    }

    public void setCantonsIncluded(List<Integer> cantonsIncluded) {
        this.cantonsIncluded = cantonsIncluded;
    }

    enum AttributeLabels {
        household_id, gender, age, household_size, municipality_type, canton_id, number_of_cars,
        driving_license, car_availability, employed, children_0_10, children_0_18, facility_id
    }

    enum Gender {female, male, unspecified}

    enum MunicipalityType {urban, suburban, rural}
}