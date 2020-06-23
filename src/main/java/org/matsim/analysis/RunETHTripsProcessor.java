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
package org.matsim.analysis;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.events.*;
import org.matsim.api.core.v01.events.handler.*;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.network.Node;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.events.MatsimEventsReader;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.core.utils.geometry.transformations.CH1903LV03PlustoWGS84;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bcharlton
 *
 */
public class RunETHTripsProcessor {
	public static final Logger log = Logger.getLogger( RunETHTripsProcessor.class ) ;
	public static int MAX_TRIPS = 15000000;
	public static int START_TIME_SECONDS = 0; // 8 * 3600; // 8am

	public static void main(String[] args) throws Exception {
		Config config = ConfigUtils.createConfig();
		config.global().setNumberOfThreads( 4 );
		System.setProperty("matsim.preferLocalDtds", "true") ;


		String networkFile = "C:\\Users\\billy\\home\\data\\snf-big-data\\from-sebastian\\switzerland_network.xml.gz";
		String eventsFile = "C:\\Users\\billy\\home\\data\\snf-big-data\\from-sebastian\\output_events.xml.gz";
		String outputCSVFile = "output/eth-trips.csv";

		//create an event object
		EventsManager events = EventsUtils.createEventsManager();

		//read network
		Network network = NetworkUtils.createNetwork();
		new MatsimNetworkReader(network).readFile(networkFile);
		System.out.println("###--- Network file read!");

		//create the handler and add it
		MyTripHandler handler1 = new MyTripHandler(network, outputCSVFile);
		events.addHandler(handler1);

		//create the reader and read the file
		MatsimEventsReader reader = new MatsimEventsReader(events);
		reader.readFile(eventsFile);
		System.out.println("###--- Events file read!");

		handler1.cleanUp();
		handler1.closeWriter();
		System.out.println("###--- JSON written!");
	}

	static class MyTripHandler implements LinkEnterEventHandler,
			LinkLeaveEventHandler, VehicleEntersTrafficEventHandler, VehicleLeavesTrafficEventHandler,
			ActivityStartEventHandler {

		MyTripHandler(Network network, String outputCSV) throws Exception {
			this.network = network;

			this.csvWriter = new BufferedWriter(new FileWriter(outputCSV));
			this.csvWriter.write("# VSP official CSV layout: time,x,y,.... and lines starting with # are comments\n");
			this.csvWriter.write("time,x,y,finishTime,finishX,finishY,distance\n");
		}

		private Network network;
		private BufferedWriter jsonWriter;
		private BufferedWriter csvWriter;

		private int counter = 0;
		private boolean isFirst = true;

		private Map<String, JSONObject> vehicles = new HashMap<>();
		private CH1903LV03PlustoWGS84 coordConverter = new CH1903LV03PlustoWGS84();
		private Map<String, VehicleTrip> vehicleTrips = new HashMap<>();

		void addEvent(String vehicleId, Node node, double timestamp) {

			double faketime = timestamp - RunETHTripsProcessor.START_TIME_SECONDS;

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

			VehicleTrip vtrip = vehicleTrips.get(vehicleId);
			vtrip.timestamps.add(faketime);
			vtrip.points.add(new ImmutablePair(wgs84.get(0), wgs84.get(1)));
			vtrip.endNode = node;
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
			if (event.getTime() < RunETHTripsProcessor.START_TIME_SECONDS) return;
			if (event.getTime() > 3600 +  RunETHTripsProcessor.START_TIME_SECONDS) return;

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
			times.add(timestamp - RunETHTripsProcessor.START_TIME_SECONDS);
			times.add(timestamp - RunETHTripsProcessor.START_TIME_SECONDS + 60);

			json.put("vendor", 1);
			json.put("path", paths);
			json.put("timestamps", times);
/*
			try {
				if (!isFirst) {
					this.jsonWriter.write(",\n");
				}
				isFirst = false;
				this.jsonWriter.write(json.toJSONString());
			} catch (Exception e) {
				// boop
			}
*/
		}

		@Override
		public void handleEvent(VehicleEntersTrafficEvent event) {
			String vehicleId = event.getVehicleId().toString();

			// test time of day: start at 8am
			// if (event.getTime() < RunTripsProcessor.START_TIME_SECONDS) return;
			// if (event.getTime() > 3600 +  RunTripsProcessor.START_TIME_SECONDS) return;

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

			VehicleTrip vtrip = new VehicleTrip();
			vtrip.startNode = node;
			vehicleTrips.put(vehicleId, vtrip);

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
			// wrote it! remove this trip from the map
			this.vehicles.remove(vehicleId);
			this.vehicleTrips.remove(vehicleId);

		}

		void writeJson(String vehicleId)  {
			JSONObject trip = vehicles.get(vehicleId);
			VehicleTrip vtrip = vehicleTrips.get(vehicleId);

			if (((JSONArray)trip.get("timestamps")).size() < 2) {
				this.vehicles.remove(vehicleId);
				this.vehicleTrips.remove(vehicleId);
				return;
			}

			try {
				/*
				if (!isFirst) {
					this.jsonWriter.write(",\n");
				}
				isFirst = false;

				this.jsonWriter.write(trip.toJSONString());
				*/
				int vtripLast = vtrip.timestamps.size() - 1;

				Coord start = vtrip.startNode.getCoord();
				Coord finish = vtrip.endNode.getCoord();
				double distance = CoordUtils.calcEuclideanDistance(start,finish);

				this.csvWriter.write(String.format("%.1f,%f,%f,%.1f,%f,%f,%f\n",
						vtrip.timestamps.get(0), vtrip.points.get(0).getLeft(), vtrip.points.get(0).getRight(),
						vtrip.timestamps.get(vtripLast), vtrip.points.get(vtripLast).getLeft(), vtrip.points.get(vtripLast).getRight(),
						distance
						));

				counter++;
				if (counter >= RunETHTripsProcessor.MAX_TRIPS) {
					closeWriter();
					System.exit(0);
				}
			} catch (Exception e) {
				System.err.println(("Could not write!"));
				System.exit(2);
			}
		}

		void cleanUp() {
			RunETHTripsProcessor.log.warn("Cleaning up " + vehicles.size() + " unfinished trips");
			for (String id : vehicles.keySet()) {
				writeJson(id);
			}
		}

		void closeWriter() throws Exception {
			this.jsonWriter.write("\n]\n");
			this.jsonWriter.close();

			this.csvWriter.close();

			RunETHTripsProcessor.log.info("Wrote " + counter + " trips!");
		}
	}

	static class VehicleTrip {
		List<Double> timestamps = new ArrayList();
		List<Pair> points = new ArrayList<>();
		Node startNode;
		Node endNode;
	};
}

