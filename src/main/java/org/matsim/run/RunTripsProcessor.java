/* *********************************************************************** *
 * project: org.matsim.*												   *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2008 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */
package org.matsim.run;

import com.google.inject.Singleton;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.events.*;
import org.matsim.api.core.v01.events.handler.*;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.*;
import org.matsim.contrib.analysis.kai.KaiAnalysisListener;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastes;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup.Algotype;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup.ApproximationLevel;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.api.internal.MatsimNetworkObject;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy.OverwriteFileSetting;
import org.matsim.core.controler.listener.StartupListener;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.MatsimEventsReader;
import org.matsim.core.gbl.Gbl;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.algorithms.TransportModeNetworkFilter;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule.DefaultSelector;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordinateTransformation;
import org.matsim.core.utils.geometry.transformations.CH1903LV03PlustoWGS84;
import org.matsim.facilities.*;
import org.matsim.vehicles.Vehicle;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.matsim.core.config.groups.PlanCalcScoreConfigGroup.ActivityParams;
import static org.matsim.core.config.groups.StrategyConfigGroup.StrategySettings;
import static org.matsim.core.config.groups.VspExperimentalConfigGroup.VspDefaultsCheckingLevel;

/**
 * @author bcharlton
 *
 */
public class RunTripsProcessor {
	public static final Logger log = Logger.getLogger( RunTripsProcessor.class ) ;
	public static int MAX_TRIPS = 15000000;
	public static int START_TIME_SECONDS = 8 * 3600; // 8am

	public static void main(String[] args) throws Exception {
		Config config = ConfigUtils.createConfig();
		config.global().setNumberOfThreads( 4 );
		System.setProperty("matsim.preferLocalDtds", "true") ;


		String eventsFile = "output_events.xml.gz";
		String networkFile = "./output_network.xml.gz";
		String outputFile = "./trips.json";

		//create an event object
		EventsManager events = EventsUtils.createEventsManager();

		//read network
		Network network = NetworkUtils.createNetwork();
		new MatsimNetworkReader(network).readFile(networkFile);
		System.out.println("###--- Network file read!");

		//create the handler and add it
		MyTripHandler handler1 = new MyTripHandler(network, outputFile);
		events.addHandler(handler1);

		//create the reader and read the file
		MatsimEventsReader reader = new MatsimEventsReader(events);
		reader.readFile(eventsFile);
		System.out.println("###--- Events file read!");

		handler1.cleanUp();
		handler1.closeWriter();
		System.out.println("###--- JSON written!");
	}
}

class MyTripHandler implements LinkEnterEventHandler,
		LinkLeaveEventHandler, VehicleEntersTrafficEventHandler, VehicleLeavesTrafficEventHandler,
		ActivityStartEventHandler {

	MyTripHandler(Network network, String output) throws Exception {
		this.network = network;
		this.writer = new BufferedWriter(new FileWriter(output));
		this.writer.write("[\n");
	}

	private Network network;
	private BufferedWriter writer;

	private int counter = 0;
	private boolean isFirst = true;

	private Map<String, JSONObject> vehicles = new HashMap<>();
	private CH1903LV03PlustoWGS84 coordConverter = new CH1903LV03PlustoWGS84();

	void addEvent(String vehicleId, Node node, double timestamp) {

		double faketime = timestamp - RunTripsProcessor.START_TIME_SECONDS;

		JSONObject v = vehicles.get(vehicleId);
		JSONArray wgs84 = this.convertToWGS84(node);

		JSONArray path = ((JSONArray)v.get("path"));
		JSONArray timestamps = ((JSONArray)v.get("timestamps"));

		// don't add duplicate nodes
		int index = path.size() - 1;
		if (index > -1 &&
			((JSONArray)path.get(index)).get(0).equals(wgs84.get(0)) &&
			((JSONArray)path.get(index)).get(1).equals(wgs84.get(1)) &&
			(double) timestamps.get(index) == faketime
		) return;

		((JSONArray)v.get("path")).add(wgs84);
		((JSONArray)v.get("timestamps")).add(faketime);
	}


	JSONArray convertToWGS84(Node node) {
		Coord c = node.getCoord();
		Coord wgs84 = coordConverter.transform(c);
		// reduce accuracy to save space
		double x = Math.floor(100000 * wgs84.getX()) / 100000.0;
		double y = Math.floor(100000 * wgs84.getY()) / 100000.0;

		JSONArray xy = new JSONArray();
		xy.add(x);
		xy.add(y);

		return xy;
	}

	@Override
	public void handleEvent(ActivityStartEvent event) {
		if (event.getTime() < RunTripsProcessor.START_TIME_SECONDS) return;
		if (event.getTime() > 3600 +  RunTripsProcessor.START_TIME_SECONDS) return;

		String activity = event.getActType();
		Id<Link> linkId = event.getLinkId();
		Node actStartNode = network.getLinks().get(linkId).getToNode();
		Node actXNode = network.getLinks().get(linkId).getFromNode();
		double timestamp = event.getTime();
		JSONArray coord = convertToWGS84(actStartNode);
		JSONArray coord2 = convertToWGS84(actXNode);

		JSONObject json = new JSONObject();

		JSONArray paths = new JSONArray();
		paths.add(coord);
		paths.add(coord);

		JSONArray times = new JSONArray();
		times.add(timestamp - RunTripsProcessor.START_TIME_SECONDS);
		times.add(timestamp - RunTripsProcessor.START_TIME_SECONDS + 60);

		json.put("vendor", 1);
		json.put("path", paths);
		json.put("timestamps", times);

		try {
			if (!isFirst) {
				this.writer.write(",\n");
			}
			isFirst = false;
			this.writer.write(json.toJSONString());
		} catch (Exception e) {
			// boop
		}
	}

	@Override
	public void handleEvent(VehicleEntersTrafficEvent event) {
		String vehicleId = event.getVehicleId().toString();

		// test time of day: start at 8am
		if (event.getTime() < RunTripsProcessor.START_TIME_SECONDS) return;
		if (event.getTime() > 3600 +  RunTripsProcessor.START_TIME_SECONDS) return;

		Id linkId = event.getLinkId();
		Link link = network.getLinks().get(linkId);
		Node node = link.getFromNode();

		// test Zurich bounding box
//		if (node.getCoord().getX() < 2673355 || node.getCoord().getX() > 2690104 ||
//		 		node.getCoord().getY() < 1245613 || node.getCoord().getY() > 1254337) return;

		JSONObject jj = new JSONObject();
		jj.put("vendor", 0);
		jj.put("path", new JSONArray());
		jj.put("timestamps", new JSONArray());
		vehicles.put(vehicleId, jj);

		addEvent(vehicleId, node, event.getTime());
	}

	@Override
	public void handleEvent(LinkEnterEvent event) {
		String vehicleId = event.getVehicleId().toString();
		if (!vehicles.containsKey(vehicleId)) return;

		Id linkId = event.getLinkId();
		Link link = network.getLinks().get(linkId);
		Node node = link.getFromNode();

		addEvent(vehicleId, node, event.getTime());
	}

	@Override
	public void handleEvent(LinkLeaveEvent event) {
		String vehicleId = event.getVehicleId().toString();
		if (!vehicles.containsKey(vehicleId)) return;

		Id linkId = event.getLinkId();
		Link link = network.getLinks().get(linkId);
		Node node = link.getToNode();

		addEvent(vehicleId, node, event.getTime());
	}

	@Override
	public void handleEvent (VehicleLeavesTrafficEvent event  ) {
		String vehicleId = event.getVehicleId().toString();
		if (!vehicles.containsKey(vehicleId)) return;

		Id linkId = event.getLinkId();
		Link link = network.getLinks().get(linkId);
		Node node = link.getFromNode();

		addEvent(vehicleId, node, event.getTime());

		this.writeJson(vehicleId);
	}

	void writeJson(String vehicleId)  {
		JSONObject trip = vehicles.get(vehicleId);
		if (((JSONArray)trip.get("timestamps")).size() < 2) {
			this.vehicles.remove(vehicleId);
			return;
		}

		try {
			if (!isFirst) {
				this.writer.write(",\n");
			}
			isFirst = false;

			this.writer.write(vehicles.get(vehicleId).toJSONString());

			counter++;
			if (counter >= RunTripsProcessor.MAX_TRIPS) {
				closeWriter();
				System.exit(0);
			}
		} catch (Exception e) {
			System.err.println(("Could not write!"));
			System.exit(2);
		}
		// wrote it! remove this trip from the map
		this.vehicles.remove(vehicleId);
	}

	void cleanUp() {
		RunTripsProcessor.log.warn("Cleaning up " + vehicles.size() + "unfinished trips");
		for (String id : vehicles.keySet()) {
			writeJson(id);
		}
	}

	void closeWriter() throws Exception {
		this.writer.write("\n]\n");
		this.writer.close();

		RunTripsProcessor.log.info("Wrote " + counter + " trips!");
	}
}
