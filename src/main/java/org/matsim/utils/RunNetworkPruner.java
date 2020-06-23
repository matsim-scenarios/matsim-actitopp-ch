package org.matsim.utils;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.NetworkWriter;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.algorithms.NetworkCleaner;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;

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
                if (linkType.toString().equals("primary_link")) continue;
                if (linkType.toString().equals("motorway_link")) continue;

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