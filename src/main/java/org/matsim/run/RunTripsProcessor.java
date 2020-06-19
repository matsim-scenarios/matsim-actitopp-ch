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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.jfree.util.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
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
import org.matsim.core.events.algorithms.EventWriterXML;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.io.MatsimNetworkReader;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.core.utils.geometry.transformations.CH1903LV03PlustoWGS84;
import org.matsim.utils.gis.shp2matsim.ShpGeometryUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.net.URL;
import java.util.*;

/**
 * @author bcharlton
 *
 */
public class RunTripsProcessor {
	public static final Logger log = Logger.getLogger( RunTripsProcessor.class ) ;
	public static int MAX_TRIPS = 1115000000;
	public static int START_TIME_SECONDS = 0; // 8 * 3600; // 8am

	public static int PERSON_SAMPLING_RATE = 1; // 25; // 1 equals all

	static EventWriterXML EVENT_WRITER;
	static EventWriterXML EVENT_WRITER_WITH_LINKS;

	public static void main(String[] args) throws Exception {
		Config config = ConfigUtils.createConfig();
		config.global().setNumberOfThreads( 4 );
		System.setProperty("matsim.preferLocalDtds", "true") ;

		String networkFile = "../../runs-svn/snf-big-data/ivt-run/switzerland_network.xml.gz";
		String shpFile = "../../shared-svn/projects/snf-big-data/data/original_files/municipalities/2018_boundaries/cantons/g2k18.shp";

		String runFolder = "../../runs-svn/snf-big-data/zh-02/";

		String eventsFile = runFolder + "output_events.xml.gz";
		String outputCSVFile = runFolder + "output/trips.csv";
		String outputActivityFile = runFolder + "output/activities.csv";
		String outputEventsFile = runFolder + "output/out-events.xml.gz";
		String outputEventsLinksFile = runFolder + "out-events-link.xml.gz";


		//create an event object
		EventsManager events = EventsUtils.createEventsManager();

		//read network
		Network network = NetworkUtils.createNetwork();
		new MatsimNetworkReader(network).readFile(networkFile);
		System.out.println("###--- Network file read!");

		//create the handler and add it
		MyTripHandler handler1 = new MyTripHandler(network, outputCSVFile, outputActivityFile, shpFile);
		events.addHandler(handler1);

		EVENT_WRITER = new EventWriterXML(outputEventsFile);
		EVENT_WRITER_WITH_LINKS = new EventWriterXML(outputEventsLinksFile);

		//create the reader and read the file
		MatsimEventsReader reader = new MatsimEventsReader(events);
		reader.readFile(eventsFile);
		System.out.println("###--- Events file read!");

		handler1.cleanUp();
		handler1.closeWriter();
		System.out.println("###--- JSON written!");
	}

	static class MyTripHandler implements PersonArrivalEventHandler, PersonDepartureEventHandler, LinkEnterEventHandler,
			LinkLeaveEventHandler, VehicleEntersTrafficEventHandler, VehicleLeavesTrafficEventHandler,
			ActivityStartEventHandler, ActivityEndEventHandler {

		MyTripHandler(Network network, String outputCSV, String outputActivity, String shpFile) throws Exception {
			this.network = network;

			this.csvWriter = new BufferedWriter(new FileWriter(outputCSV));
			this.csvWriter.write("time,x,y,finishTime,finishX,finishY,distance,personId,actStart,actEnd\n");

			this.activityWriter = new BufferedWriter(new FileWriter(outputActivity));
			this.activityWriter.write("time,lon,lat,finishTime,personId,actType\n");

			this.geom = ShpGeometryUtils.loadPreparedGeometries(new URL("file://" + shpFile));
		}

		private Network network;
		private BufferedWriter csvWriter, activityWriter;

		private List<PreparedGeometry> geom;

		private int counter = 0;

		private Map<String, JSONObject> vehicles = new HashMap<>();
		private CH1903LV03PlustoWGS84 coordConverter = new CH1903LV03PlustoWGS84();
		private Map<String, VehicleTrip> vehicleTrips = new HashMap<>();

		private Set<String> activeVehicles = new HashSet<>();
		private Map<String, String> personActivities = new HashMap<>();
		private Map<String, Activity> personActivityDetails = new HashMap<>();

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

		Coord convertWGS84Coord(Node node) {
			Coord c = node.getCoord();
			Coord wgs84 = coordConverter.transform(c);
			return wgs84;
		}

		@Override
		public void handleEvent(PersonDepartureEvent event) {
			String personId = event.getPersonId().toString();

			// Ignore freight events
			if (personId.startsWith("freight")) return;

			// Ignore events if person is not being sampled
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			// test time of day: start at 8am
			// if (event.getTime() < RunTripsProcessor.START_TIME_SECONDS) return;
			// if (event.getTime() > 3600 +  RunTripsProcessor.START_TIME_SECONDS) return;

			Id linkId = event.getLinkId();
			Link link = network.getLinks().get(linkId);

			// if there is no link, then person doesn't travel? Ignore them
			if (link == null) return;

			Node node = link.getFromNode();

			// test Zurich bounding box
			//		if (node.getCoord().getX() < 2673355 || node.getCoord().getX() > 2690104 ||
			//		 		node.getCoord().getY() < 1245613 || node.getCoord().getY() > 1254337) return;

			JSONObject jj = new JSONObject();
			jj.put("vendor", 0);
			jj.put("path", new JSONArray());
			jj.put("timestamps", new JSONArray());
			vehicles.put(personId, jj);

			VehicleTrip vtrip = new VehicleTrip();
			vtrip.startNode = node;

			if (personActivities.containsKey(personId)) {
				vtrip.actStart = personActivities.get(personId);
			}
			vehicleTrips.put(personId, vtrip);

			addEvent(personId, node, event.getTime());
			EVENT_WRITER.handleEvent(event);
			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent (PersonArrivalEvent event  ) {
			String personId = event.getPersonId().toString();

			if (!vehicles.containsKey(personId)) return;

			// Ignore freight events
			if (personId.startsWith("freight")) return;

			// Ignore events if person is not being sampled
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			Id linkId = event.getLinkId();
			try {
				Link link = network.getLinks().get(linkId);
				Node node = link.getFromNode();
				addEvent(personId, node, event.getTime());
//				this.writeJson(personId, false);

			} catch (NullPointerException npe) {
				RunTripsProcessor.log.warn("Person ID " + personId + " ended without a node/link");
			}

			EVENT_WRITER.handleEvent(event);
			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		void writeJson(String vehicleId, boolean cleanup)  {
			JSONObject trip = vehicles.get(vehicleId);
			VehicleTrip vtrip = vehicleTrips.get(vehicleId);

//			if (((JSONArray)trip.get("timestamps")).size() < 2 && !cleanup) {
//				this.vehicles.remove(vehicleId);
//				this.vehicleTrips.remove(vehicleId);
//				return;
//			}

			try {
				int vtripLast = vtrip.timestamps.size() - 1;

				Coord start = vtrip.startNode.getCoord();
				Coord finish = vtrip.endNode.getCoord();
				double distance = CoordUtils.calcEuclideanDistance(start, finish);

				// let's figure out the district
//				ShpGeometryUtils.isCoordInPreparedGeometries(start, this.geom);
//				if (!ShpGeometryUtils.isCoordInPreparedGeometries(finish, this.geom)) {
//					log.warn("coord not in geom: " + finish.toString());
//					this.geom.get(0).
//				}

				// Use Locale.ROOT to force period as decimal separator
				this.csvWriter.write(String.format(Locale.ROOT,
						"%.1f,%f,%f,%.1f,%f,%f,%f,%s,%s,%s\n",
						vtrip.timestamps.get(0),
						vtrip.points.get(0).getLeft(), vtrip.points.get(0).getRight(),
						vtrip.timestamps.get(vtripLast),
						vtrip.points.get(vtripLast).getLeft(), vtrip.points.get(vtripLast).getRight(),
						distance,
						vehicleId,
						vtrip.actStart,
						vtrip.actEnd
						));

				counter++;
				if (counter >= RunTripsProcessor.MAX_TRIPS) {
					closeWriter();
					System.exit(0);
				}
			} catch (Exception e) {
				System.err.println(("Could not write!" + vehicleId));
				// System.exit(2);
			}
			// wrote it! remove this trip from the map (if we're not already looping on the map)
			if (!cleanup) {
				this.vehicles.remove(vehicleId);
				this.vehicleTrips.remove(vehicleId);

			}

		}

		void cleanUp() {
			RunTripsProcessor.log.warn("Cleaning up " + vehicles.size() + " unfinished trips");
			for (String id : vehicles.keySet()) {
				try {
					writeJson(id, true);
				} catch (Exception e) {
					// meh who cares
				}
			}

			RunTripsProcessor.log.warn("Not cleaning up " + personActivityDetails.size() + " unfinished activities");

//			for (String key: personActivityDetails.keySet()) {
//				Activity a = personActivityDetails.get(key);
//				RunTripsProcessor.log.info(a);
//			}

		}

		void closeWriter() throws Exception {
			this.csvWriter.close();
			this.activityWriter.close();

			RunTripsProcessor.log.info("Wrote " + counter + " trips!");
		}

		@Override
		public void handleEvent(ActivityStartEvent event) {
			// Ignore events if person is not being sampled
			String personId = event.getPersonId().toString();
			if (personId.startsWith("freight")) return;
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			if (vehicleTrips.containsKey((personId))) {
				VehicleTrip vtrip = vehicleTrips.get(personId);
				String actEnd = event.getActType();
				vtrip.actEnd = actEnd;
			}

			Activity a = new Activity();
			a.personId = personId;
			a.timeStart = event.getTime();
			a.actType = event.getActType();

			// figure out location - startnode? getCoord returns null :-(
			try {
				Id linkId = event.getLinkId();
				Link l = this.network.getLinks().get(linkId);
				Coord wgs84 = convertWGS84Coord(l.getFromNode());
				a.location=new ImmutablePair(wgs84.getX(), wgs84.getY());
			} catch (Exception e) {
				// who cares
			}

			personActivityDetails.put(personId, a);

			this.writeJson(personId, false);

			EVENT_WRITER.handleEvent(event);
			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent(LinkEnterEvent event) {
			// Ignore events if person is not being sampled
			String vehId = event.getVehicleId().toString();
			if (!this.activeVehicles.contains(vehId)) return;

			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent(LinkLeaveEvent event) {
			// Ignore events if person is not being sampled
			String vehId = event.getVehicleId().toString();
			if (!this.activeVehicles.contains(vehId)) return;

			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent(VehicleEntersTrafficEvent event) {
			// Ignore events if person is not being sampled
			String personId = event.getPersonId().toString();
			if (personId.startsWith("freight")) return;
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			String vehId = event.getVehicleId().toString();
			this.activeVehicles.add(vehId);

			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent(VehicleLeavesTrafficEvent event) {
			// Ignore events if person is not being sampled
			String personId = event.getPersonId().toString();
			if (personId.startsWith("freight")) return;
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			String vehId = event.getVehicleId().toString();
			this.activeVehicles.remove(vehId);

			EVENT_WRITER_WITH_LINKS.handleEvent(event);
		}

		@Override
		public void handleEvent(ActivityEndEvent event) {
			String personId = event.getPersonId().toString();
			if (personId.startsWith("freight")) return;
			if (Integer.parseInt(personId) % PERSON_SAMPLING_RATE != 0) return;

			personActivities.put(personId, event.getActType());

			if (personActivityDetails.containsKey(personId)) {
				try {
					Activity a = personActivityDetails.get(personId);
					this.activityWriter.write(String.format(Locale.ROOT,
						"%.1f,%.5f,%.5f,%.1f,%s,%s\n",
						a.timeStart,
						a.location.getLeft(), a.location.getRight(),
						event.getTime(),
						a.personId,
						a.actType));

					personActivityDetails.remove(personId);
				} catch (Exception e) {
					// who cares
				}
			}

		}
	}

	static class VehicleTrip {
		List<Double> timestamps = new ArrayList();
		List<Pair> points = new ArrayList<>();
		Node startNode;
		Node endNode;
		String actStart = "";
		String actEnd = "";
	};

	static class Activity {
		String personId;
		Pair location;
		Double timeStart;
		Double timeEnd;
		String actType;

		public String toString() {
			return personId + "," + timeStart +"," + actType ;
		}
	}
}

