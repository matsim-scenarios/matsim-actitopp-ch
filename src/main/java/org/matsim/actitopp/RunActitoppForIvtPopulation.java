package org.matsim.actitopp;

import edu.kit.ifv.mobitopp.actitopp.*;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Point;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.algorithms.TransportModeNetworkFilter;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.population.io.PopulationReader;
import org.matsim.core.router.FastDijkstraFactory;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.core.utils.geometry.GeometryUtils;
import org.matsim.core.utils.gis.ShapeFileReader;
import org.matsim.counts.Count;
import org.matsim.counts.Counts;
import org.matsim.counts.MatsimCountsReader;
import org.matsim.counts.Volume;
import org.matsim.facilities.ActivityFacilities;
import org.matsim.facilities.ActivityFacility;
import org.matsim.facilities.MatsimFacilitiesReader;
import org.matsim.utils.objectattributes.attributable.Attributes;
import org.opengis.feature.simple.SimpleFeature;
import playground.vsp.openberlinscenario.cemdap.input.CEMDAPPersonAttributes;

import java.util.*;

/**
 * @author dziemke
 */
public class RunActitoppForIvtPopulation {
    private static final Logger LOG = Logger.getLogger(RunActitoppForIvtPopulation.class);
    private static ModelFileBase fileBase = new ModelFileBase();
    private static RNGHelper randomgenerator = new RNGHelper(1234);
	private final HashMap<Integer, SimpleFeature> mmm = new HashMap<>(  ) ;

	Map<Integer, Coord> municipalityCenters;
    Random random = MatsimRandom.getLocalInstance();
    LeastCostPathCalculator leastCostPathCalculator;
    private Scenario scenario;
	private Map<Integer, List<Integer>> observedCommutes;

    public RunActitoppForIvtPopulation(Scenario scenario, String municipalitiesShapeFile, String countsFile,
                                       int beginReprTimePeriod, int endReprTimePeriod){
	    this.scenario = scenario;

	    prepareObservedCommutes( countsFile, beginReprTimePeriod, endReprTimePeriod );
	    createMunicipalityCenterMap( municipalitiesShapeFile );

	    FreespeedTravelTimeAndDisutility freeSpeed = new FreespeedTravelTimeAndDisutility( scenario.getConfig().planCalcScore() );
	    leastCostPathCalculator = (new FastDijkstraFactory()).createPathCalculator( scenario.getNetwork(), freeSpeed, freeSpeed );

	    for( SimpleFeature feature : ShapeFileReader.getAllFeatures( municipalitiesShapeFile ) ){
		    int municapalityId = Integer.valueOf(feature.getAttribute("GMDNR").toString());
		    mmm.put( municapalityId, feature ) ;
	    }
    }

    public static void main(String[] args) {
        // Input and output files
        String folderRoot = "/C:/Users/billy/shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/";
        String populationFile = folderRoot + "population_1pct.xml.gz";
        String facilitiesFile = folderRoot + "facilities_1pct.xml.gz";
        String networkFile = "/C:/Users/billy/shared-svn/projects/snf-big-data/data/scenario/transport_supply/switzerland_network.xml.gz";

        String municipalitiesShapeFile = "/C:/Users/billy/shared-svn/projects/snf-big-data/data/original_files/municipalities/2018_boundaries/g2g18.shp";
        String countsFile = "/C:/Users/billy/shared-svn/projects/snf-big-data/data/commute_counts/20161001_neuenburg_2018_1pct.xml.gz";
        int beginReprTimePeriod = 6;
        int endReprTimePeriod = 10;

        String populationScheduleFile = folderRoot + "population_1pct_plans_initial-coords.xml.gz";

        // Create scenario
        MutableScenario scenario = ScenarioUtils.createMutableScenario(ConfigUtils.createConfig());
        new PopulationReader(scenario).readFile(populationFile);
        new MatsimFacilitiesReader(scenario).readFile(facilitiesFile);
        new MatsimNetworkReader(scenario.getNetwork()).readFile(networkFile);
        TransportModeNetworkFilter filter = new TransportModeNetworkFilter(scenario.getNetwork());
        Network carNetwork = NetworkUtils.createNetwork();
        Set<String> modeSet = new HashSet<>();
        modeSet.add(TransportMode.car);
        filter.filter(carNetwork, modeSet);
        scenario.setNetwork(carNetwork);

        // Script
        RunActitoppForIvtPopulation ivtPopulationScheduler = new RunActitoppForIvtPopulation(scenario, municipalitiesShapeFile,
                countsFile, beginReprTimePeriod, endReprTimePeriod);
        ivtPopulationScheduler.runActitopp();
        ivtPopulationScheduler.writeMatsimPlansFile(scenario.getPopulation(), populationScheduleFile);
    }


//    private void findMUnicipality

    // Information from "https://github.com/mobitopp/actitopp"
    // 1 = full-time occupied; 2 = half-time occupied; 3 = not occupied; 4 = student (school or university);
    // 5 = worker in vocational program; 7 = retired person / pensioner
    private static int getEmploymentClassSwitzerland(boolean employed, int age) {
        Random random = MatsimRandom.getLocalInstance();
        int employmentClass = -1;
        if (employed) {
            double randomNumber = random.nextDouble();
            if (randomNumber < 0.7) { // TODO substantiate this figure
                employmentClass = 1;
            } else if (randomNumber >= 0.7 && randomNumber < 0.98) { // TODO substantiate this figure
                employmentClass = 2;
            } else {
                employmentClass = 5;
            }
        } else {
            double randomNumber = random.nextDouble();
            if (age > 65) { // TODO substantiate this figure
                employmentClass = 7;
            } else if (age < 25) { // TODO substantiate this figure
                employmentClass = 4;
            } else {
                employmentClass = 5;
            }
        }
        return employmentClass;
    }

    // Information from "https://github.com/mobitopp/actitopp"
    // 1 = rural; 2 = provincial; 3 = cityoutskirt; 4 = metropolitan; 5 = conurbation
    // 5 = >500000 im Regionkern
    // 4 = 50000-500000 im Regionskern
    // 3 = >50000 am Regionsrand
    // 2 = 5000-50000
    // 1 = < 5000
    // Based on BIK regions, cf. MOP
    private static int getAreaTypeSwitzerland(IvtPopulationParser.MunicipalityType municipalityType) {
        int areaType = -1;
        if (IvtPopulationParser.MunicipalityType.urban == municipalityType) {
            areaType = 4;
        } else if (IvtPopulationParser.MunicipalityType.suburban == municipalityType) {
            areaType = 3;
        } else if (IvtPopulationParser.MunicipalityType.rural == municipalityType) {
            areaType = 1;
        }
        return areaType;
    }

    // Information from "https://github.com/mobitopp/actitopp/blob/master/src/main/java/edu/kit/ifv/mobitopp/actitopp/Configuration.java"
    private static String transformActType(ActivityType activityTypeLetter) {
        if (activityTypeLetter == ActivityType.HOME) {
            return ActiToppActivityTypes.home.toString();
        } else if (activityTypeLetter == ActivityType.WORK) {
            return ActiToppActivityTypes.work.toString();
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

    public void runActitopp() {
        Population population = scenario.getPopulation();
        ActivityFacilities facilities = scenario.getActivityFacilities();
        for (Person matsimPerson : population.getPersons().values()) {
            Id<ActivityFacility> facId = Id.create(matsimPerson.getAttributes().getAttribute(IvtPopulationParser.AttributeLabels.facility_id.toString()).toString(), ActivityFacility.class);
            Coord homeCoord = facilities.getFacilities().get(facId).getCoord();

            ActitoppPerson actitoppPerson = createActitoppPersonSwitzerland(matsimPerson, homeCoord);

            HWeekPattern weekPattern = createActitoppWeekPattern(actitoppPerson);
            Plan matsimPlan = createMatsimPlan(matsimPerson, weekPattern, population, homeCoord);
            matsimPerson.addPlan(matsimPlan);
        }
    }

    private void prepareObservedCommutes(String countsFile, int beginReprTimePeriod, int endReprTimePeriod) {
        LOG.info("Start creating map with observed commutes.");
        Counts commuteCounts = new Counts();
        MatsimCountsReader countsReader = new MatsimCountsReader(commuteCounts);
        countsReader.readFile(countsFile);
        this.observedCommutes = new HashMap<>();

        for (Object uncastedCount : commuteCounts.getCounts().values()) {
            Count count = (Count) uncastedCount;
            String[] from_to = count.getId().toString().split("_");
            int from = Integer.parseInt(from_to[0]);
            int to = Integer.parseInt(from_to[1]);
            for (Object uncastedVolume : count.getVolumes().values()) {
                Volume volume = (Volume) uncastedVolume;
                if (beginReprTimePeriod < volume.getHourOfDayStartingWithOne() && volume.getHourOfDayStartingWithOne() <= endReprTimePeriod) {
                    double value = volume.getValue();
                    List<Integer> destinations;
                    if (!observedCommutes.keySet().contains(from)) {
                        destinations = new ArrayList<>();
                        observedCommutes.put(from, destinations);
                    }
                    destinations = observedCommutes.get(from);
                    for (int i = 0; i < value; i++) {
                        destinations.add(to);
                    }
                }
            }
        }
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
        return actitoppPerson.getWeekPattern();
    }

    private void createMunicipalityCenterMap(String municipalitiesShapeFile) {
        LOG.info("Start creating municipality center map.");
        this.municipalityCenters = new HashMap<>();

        ShapeFileReader sfr = new ShapeFileReader();
        for (SimpleFeature sf : sfr.getAllFeatures(municipalitiesShapeFile)) {
            int municapalityId = Integer.valueOf(sf.getAttribute("GMDNR").toString());
            int zentrumskoordindateEast = Integer.valueOf(sf.getAttribute("E_CNTR").toString());
            int zentrumskoordindateNorth = Integer.valueOf(sf.getAttribute("N_CNTR").toString());
            municipalityCenters.put(municapalityId, CoordUtils.createCoord(zentrumskoordindateEast, zentrumskoordindateNorth));
        }
    }

    private ActitoppPerson createActitoppPersonSwitzerland(Person matsimPerson, Coord homeCoord) {
        // TODO Find out if we should include houeholds here (also ask Tim)
        int personIndex = Integer.parseInt(matsimPerson.getId().toString());
        Attributes attr = matsimPerson.getAttributes();

        int childrenFrom0To10 = getIntFromBoolean((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.children_0_10.toString()));
        int childrenUnder18 = getIntFromBoolean((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.children_0_18.toString()));

        int age = (int) attr.getAttribute(CEMDAPPersonAttributes.age.toString());

        int employment = getEmploymentClassSwitzerland((boolean) attr.getAttribute(IvtPopulationParser.AttributeLabels.employed.toString()), age); // TODO Substantiate asumptions
        attr.putAttribute(ActitoppAttributeLabels.actitopp_employment_class.toString(), employment);

        int gender = getGenderClassSwitzerland(IvtPopulationParser.Gender.valueOf((String) attr.getAttribute(IvtPopulationParser.AttributeLabels.gender.toString())));
        attr.putAttribute(ActitoppAttributeLabels.actitopp_gender.toString(), gender);

        int areaType = getAreaTypeSwitzerland(IvtPopulationParser.MunicipalityType.valueOf((String) attr.getAttribute(IvtPopulationParser.AttributeLabels.municipality_type.toString())));
        attr.putAttribute(ActitoppAttributeLabels.actitopp_area_type.toString(), areaType);

        int numberOfCarsInHousehold = (int) attr.getAttribute(IvtPopulationParser.AttributeLabels.number_of_cars.toString());

        int homeMunicipality = (int) attr.putAttribute(IvtPopulationParser.AttributeLabels.municipality_id.toString(), employment);
        int destination;
        double commutingDistanceToWork = 0;
        double commutingDistanceToEducation = 0; // TODO

        // 1 = full-time occupied; 2 = half-time occupied; 3 = not occupied; 4 = student (school or university);
        // 5 = worker in vocational program; 7 = retired person / pensioner
        if (employment == 1 || employment == 2 || employment == 4 || employment == 5) {
            List<Integer> outgoingCommutes = observedCommutes.get(homeMunicipality);
            int randomInt = random.nextInt(outgoingCommutes.size());
            destination = outgoingCommutes.get(randomInt);
            outgoingCommutes.remove(randomInt);
            if (employment == 1 || employment == 2 || employment == 5) {
                commutingDistanceToWork = getCommutingDistance(homeCoord, destination);
                attr.putAttribute(ActitoppAttributeLabels.work_edu_municipality_id.toString(), destination);
            } else if (employment == 4) {
                commutingDistanceToEducation = getCommutingDistance(homeCoord, destination);
                attr.putAttribute(ActitoppAttributeLabels.work_edu_municipality_id.toString(), destination);
            }
        }

        ActitoppPerson actitoppPerson = new ActitoppPerson(personIndex, childrenFrom0To10, childrenUnder18, age,
                employment, gender, areaType, numberOfCarsInHousehold, commutingDistanceToWork, commutingDistanceToEducation);
        return actitoppPerson;
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
        } else if (IvtPopulationParser.Gender.unspecified == gender) {
            if (random.nextDouble() > 0.5) {
                genderClass = 1; // male
            } else {
                genderClass = 2; // female
            }
        }
        return genderClass;
    }

    private double getCommutingDistance(Coord homeCoord, int destination) {
        double distance = 0.;
        Node homeNode = NetworkUtils.getNearestNode(scenario.getNetwork(), homeCoord);
        Node destinationNode = NetworkUtils.getNearestNode(scenario.getNetwork(), municipalityCenters.get(destination));

        LeastCostPathCalculator.Path path = leastCostPathCalculator.calcLeastCostPath(
                homeNode, destinationNode, 8 * 60. * 60., null, null);
        for (Link link : path.links) {
            distance += link.getLength();
        }
        return distance / 1000.;
    }

    private Plan createMatsimPlan(Person matsimPerson, HWeekPattern weekPattern, Population population, Coord homeCoord) {
        PopulationFactory populationFactory = population.getFactory();
        Plan matsimPlan = populationFactory.createPlan();

        List<HActivity> activityList = weekPattern.getAllActivities();
        for (HActivity actitoppActivity : activityList) {
            if (actitoppActivity.getDayIndex() == 0) { // Only use activities of first day; until 1,440min
                // actitoppActivity.getType(); // Letter-based type
                actitoppActivity.getActivityType();
                String matsimActivityType = transformActType(actitoppActivity.getActivityType());
                Coord coord;
                if (matsimActivityType.equals(ActiToppActivityTypes.home.toString())) {
                    coord = homeCoord;
                } else if (matsimActivityType.equals(ActiToppActivityTypes.work.toString()) || matsimActivityType.equals(ActiToppActivityTypes.education.toString())) {
                    if (matsimPerson.getAttributes().getAttribute(ActitoppAttributeLabels.work_edu_municipality_id.toString()) != null) {
                        int workEduMunId = (int) matsimPerson.getAttributes().getAttribute(ActitoppAttributeLabels.work_edu_municipality_id.toString());
                        // coord = municipalityCenters.get(workEduMunId); // Don't use municipality center anymore; pick a random point within the municipality.
                        Point point = GeometryUtils.getRandomPointInFeature( MatsimRandom.getRandom(), mmm.get( workEduMunId ) );;
                        coord = new Coord( point.getX(), point.getY() ) ;
                    } else { // This the case when someone performs a work or education activity who is not expected so based on his employment status
                        int homeMunId = (int) matsimPerson.getAttributes().getAttribute(IvtPopulationParser.AttributeLabels.municipality_id.toString());
                        // coord = municipalityCenters.get(homeMunId);  // ^^ same
                        Point point = GeometryUtils.getRandomPointInFeature( MatsimRandom.getRandom(), mmm.get( homeMunId ) );;
                        coord = new Coord( point.getX(), point.getY() ) ;
                    }
                } else {
                    coord = homeCoord; // Just as an initial guess
                }

                Activity matsimActivity = populationFactory.createActivityFromCoord(matsimActivityType, coord);
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

    private void writeMatsimPlansFile(Population population, String fileName) {
        PopulationWriter popWriter = new PopulationWriter(population);
        popWriter.write(fileName);
    }

    enum ActitoppAttributeLabels {
        actitopp_employment_class, actitopp_gender, actitopp_area_type,
        work_edu_municipality_id
    }
}
