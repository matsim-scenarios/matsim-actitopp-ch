package org.matsim.actitopp;

import edu.kit.ifv.mobitopp.actitopp.*;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.DefaultActivityTypes;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.population.io.PopulationReader;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.utils.objectattributes.attributable.Attributes;
import playground.vsp.openberlinscenario.cemdap.input.CEMDAPPersonAttributes;

import java.util.List;
import java.util.Random;

/**
 * @author dziemke
 */
public class RunActitoppForIvtPopulation {
    private static final Logger LOG = Logger.getLogger(RunActitoppForIvtPopulation.class);
    private static ModelFileBase fileBase = new ModelFileBase();
    ;
    private static RNGHelper randomgenerator = new RNGHelper(1234);

    public static void main(String[] args) {
        // Input and output files
        String folderRoot = "../../svn/shared-svn/projects/snf-big-data/data/scenario/";
        String populationFile = folderRoot + "population_1pct.xml.gz";
        String populationScheduleFile = folderRoot + "population_plans_1pct.xml.gz";

        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        PopulationReader reader = new PopulationReader(scenario);
        reader.readFile(populationFile);

        runActitopp(scenario.getPopulation());
        writeMatsimPlansFile(scenario.getPopulation(), populationScheduleFile);
    }

    private static void runActitopp(Population population) {
        for (Person matsimPerson : population.getPersons().values()) {
            ActitoppPerson actitoppPerson = createActitoppPersonSwitzerland(matsimPerson);
            HWeekPattern weekPattern = createActitoppWeekPattern(actitoppPerson);

            PopulationFactory populationFactory = population.getFactory();
            matsimPerson.addPlan(createMatsimPlan(weekPattern, populationFactory));
        }
    }

    private static ActitoppPerson createActitoppPersonSwitzerland(Person matsimPerson) {
        int personIndex = Integer.parseInt(matsimPerson.getId().toString());
        Attributes attr = matsimPerson.getAttributes();

        int childrenFrom0To10 = getIntFromBoolean((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.children_0_10.toString()));
        int childrenUnder18 = getIntFromBoolean((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.children_0_18.toString()));

        int age = (int) attr.getAttribute(CEMDAPPersonAttributes.age.toString());

        // TODO Improve
        int employment = getEmploymentClassSwitzerland((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.employed.toString()));
        attr.putAttribute(ActitoppAttributeLabels.actitopp_employment_class.toString(), employment);

        int gender = getGenderClassSwitzerland(IvtPopulationParser.Gender.valueOf((String) attr.getAttribute(IvtPopulationParser.AttributeLabels.gender.toString())));
        attr.putAttribute(ActitoppAttributeLabels.actitopp_gender.toString(), gender);

        int areaType = getAreaType(); // TODO

        int numberOfCarsInHousehold = (int) attr.getAttribute(IvtPopulationParser.AttributeLabels.number_of_cars.toString());

        double commutingDistanceToWork = 20.;
        double commutingDistanceToEducation = 20.;

        ActitoppPerson actitoppPerson = new ActitoppPerson(personIndex, childrenFrom0To10, childrenUnder18, age,
                employment, gender, areaType, numberOfCarsInHousehold, commutingDistanceToWork,
                commutingDistanceToEducation);
        return actitoppPerson;
    }

    private static int getIntFromBoolean(boolean value) {
        int result = 0;
        if (value) {
            result = 1;
        }
        return result;
    }

    private static HWeekPattern createActitoppWeekPattern(ActitoppPerson actitoppPerson) {
        boolean scheduleOK = false;
        while (!scheduleOK) {
            try {
                // create weekly activity plan
                actitoppPerson.generateSchedule(fileBase, randomgenerator);
                scheduleOK = true;
            } catch (InvalidPatternException e) {
                System.err.println(e.getReason());
                System.err.println("person involved: " + actitoppPerson.getPersIndex());
            }
        }
        // actitoppPerson.getWeekPattern().printAllActivitiesList();
        return actitoppPerson.getWeekPattern();
    }

    private static Plan createMatsimPlan(HWeekPattern weekPattern, PopulationFactory populationFactory) {
        Plan matsimPlan = populationFactory.createPlan();

        List<HActivity> activityList = weekPattern.getAllActivities();
        for (HActivity actitoppActivity : activityList) {
            if (actitoppActivity.getDayIndex() == 0) { // Only use activities of first day; until 1,440min
                // actitoppActivity.getType(); // Letter-based type
                actitoppActivity.getActivityType();
                String matsimActivityType = transformActType(actitoppActivity.getActivityType());
                Coord dummyCoord = CoordUtils.createCoord(0, 0); // TODO choose location

                Activity matsimActivity = populationFactory.createActivityFromCoord(matsimActivityType, dummyCoord);
                matsimPlan.addActivity(matsimActivity);

                int activityEndTime_min = actitoppActivity.getEndTime();
                if (activityEndTime_min <= 24 * 60) { // i.e. midnight in minutes
                    matsimActivity.setEndTime(activityEndTime_min * 60); // times in ActiTopp in min, in MATSim in s

                    Leg matsimLeg = populationFactory.createLeg(TransportMode.car); // TODO
                    matsimPlan.addLeg(matsimLeg);
                }
            }
        }
        return matsimPlan;
    }

    // Information from "https://github.com/mobitopp/actitopp"
    // 1 = full-time occupied; 2 = half-time occupied; 3 = not occupied; 4 = student (school or university);
    // 5 = worker in vocational program; 7 = retired person / pensioner
    private static int getEmploymentClassSwitzerland(boolean employed) {
        int employmentClass = -1;
        if (employed) {
            employmentClass = 1; // TODO distinguish between full- and half-time occupation
            // Tim, Michael H. also estimate this in a model for people outside Karlsruhe
        } else {
            employmentClass = 3;
        }
        // TODO "worker in vocational program", "student", and "retired person / pensioner" are not yet considered
        return employmentClass;
    }

    // Information from "https://github.com/mobitopp/actitopp"
    // 1 = male; 2 = female
    private static int getGenderClassSwitzerland(IvtPopulationParser.Gender gender) {
        Random random = MatsimRandom.getLocalInstance();
        int genderClass = -1;
        if (IvtPopulationParser.Gender.male == gender) {
            genderClass = 1;
        } else if (IvtPopulationParser.Gender.female == gender) {
            genderClass = 2;
        } else if (IvtPopulationParser.Gender.female == gender) {
            if (random.nextDouble() > 0.5) {
                genderClass = 1; // male
            } else {
                genderClass = 2; // female
            }
        }
        return genderClass;
    }

    // PKW-Besitzquote pro Gemeinde
    private static int getNumberOfCarsInHousehold() {
        return 0;
    }

    // Information from "https://github.com/mobitopp/actitopp"
    // 1 = rural; 2 = provincial; 3 = cityoutskirt; 4 = metropolitan; 5 = conurbation
    // 5 = >500000 im Regionkern
    // 4 = 50000-500000 im Regionskern
    // 3 = >50000 am Regionsrand
    // 2 = 5000-50000
    // 1 = < 5000
    // Based on BIK regions, cf. MOP
    private static int getAreaType() {
        // TODO Right now everybody is "metropolitan"
        return 4;
    }

    // Information from "https://github.com/mobitopp/actitopp"
    // Commuting distance to work in kilometers (0 if non existing) or commuting distance to school/university in kilometers (0 if non existing)
    private static double getDistanceEstimate(int householdId, int destinationZoneId) {
        // TODO Right now everybody makes trips of 5 kilometers
        return 5.;
    }

    // Information from "https://github.com/mobitopp/actitopp/blob/master/src/main/java/edu/kit/ifv/mobitopp/actitopp/Configuration.java"
    private static String transformActType(ActivityType activityTypeLetter) {
        if (activityTypeLetter == ActivityType.HOME) {
            return DefaultActivityTypes.home;
        } else if (activityTypeLetter == ActivityType.WORK) {
            return DefaultActivityTypes.work;
        } else if (activityTypeLetter == ActivityType.EDUCATION) {
            return ActiToppActivityTypes.education.toString();
        } else if (activityTypeLetter == ActivityType.LEISURE) {
            return ActiToppActivityTypes.leisure.toString();
        } else if (activityTypeLetter == ActivityType.SHOPPING) {
            return ActiToppActivityTypes.shopping.toString();
        } else if (activityTypeLetter == ActivityType.TRANSPORT) {
            return ActiToppActivityTypes.other.toString();
        } else {
            LOG.error(new IllegalArgumentException("Activity type " + activityTypeLetter + " not allowed."));
            return null;
        }
    }

    private static void writeMatsimPlansFile(Population population, String fileName) {
        PopulationWriter popWriter = new PopulationWriter(population);
        popWriter.write(fileName);
    }

    enum ActitoppAttributeLabels {actitopp_employment_class, actitopp_gender, actitopp_area_type}
}