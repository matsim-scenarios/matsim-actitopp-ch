package org.matsim.run;

import edu.kit.ifv.mobitopp.actitopp.*;
import org.apache.log4j.Logger;
import org.jfree.util.Log;
import org.matsim.actitopp.ActiToppActivityTypes;
import org.matsim.actitopp.IvtPopulationParser;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkWriter;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.algorithms.NetworkCleaner;
import org.matsim.core.network.algorithms.TransportModeNetworkFilter;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.population.io.PopulationReader;
import org.matsim.core.router.FastDijkstraFactory;
import org.matsim.core.router.costcalculators.FreespeedTravelTimeAndDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
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
 * @author bcharlton
 */
public class RunNetworkPruner {
    private static final Logger LOG = Logger.getLogger(RunNetworkPruner.class);

    Random random = MatsimRandom.getLocalInstance();
    LeastCostPathCalculator leastCostPathCalculator;
    private Map<Integer, List<Integer>> observedCommutes;

    public RunNetworkPruner() {}

    public static void main(String[] args) {
        String folderRoot = "../../shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/";
        String networkFile = "../../shared-svn/projects/snf-big-data/data/scenario/transport_supply/switzerland_network.xml.gz";
        String prunedNetworkFile = folderRoot + "pruned_neuenburg_network.xml.gz";

        // Create scenario
        MutableScenario scenario = ScenarioUtils.createMutableScenario(ConfigUtils.createConfig());
        new MatsimNetworkReader(scenario.getNetwork()).readFile(networkFile);
        Network network = scenario.getNetwork();

        int outsideBoundingBox = 0;
        int tooSmall = 0;

        ArrayList<Link> pruneList = new ArrayList<>();

        // Loop over links
        for (Link link: network.getLinks().values()) {
            // Remove far-away links; bounding box 6.43-7.28, 46.75-47.12
            // (Neuenburgsee is lnglat: 6.909, 46.968)
            // 2519347, 1177185  // 2587230, 1227888
            int lowerx = 2519347;
            int upperx = 2587230;
            int lowery = 1177185;
            int uppery = 1227888;

            Coord coord = link.getFromNode().getCoord();
            if (coord.getX() < lowerx || coord.getX() > upperx || coord.getY() < lowery || coord.getY() > uppery) {

                // well, let's not prune highways outside the box
                Object linkType = link.getAttributes().getAttribute("osm:way:highway");
                if (linkType==null) continue;
                if (linkType.toString().equals("motorway")) continue;
                if (linkType.toString().equals("primary")) continue;

                pruneList.add(link);
                outsideBoundingBox++;
            }

            // Remove smaller links
            if (link.getAttributes().getAttribute("osm:way:highway")==null) continue;

            switch (link.getAttributes().getAttribute("osm:way:highway").toString()) {
                case "residential":
                //case "tertiary":
                //case "unspecified":
                    pruneList.add(link);
                    tooSmall++;
                    break;
                default:
                    continue;
            }
        }

        LOG.info("Pruning " + tooSmall + " minor links ");
        LOG.info("Pruning " + outsideBoundingBox + " links outside bounding box");

        for (Link l: pruneList) {
            network.removeLink(l.getId());
        }

        LOG.info("Running NetworkCleaner");

        NetworkCleaner nc = new NetworkCleaner();
        nc.run(network);

        LOG.info("Writing " + prunedNetworkFile);
        new NetworkWriter(network).write(prunedNetworkFile);
    }
}