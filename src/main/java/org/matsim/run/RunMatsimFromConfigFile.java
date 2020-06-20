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

import ch.sbb.matsim.routing.pt.raptor.SwissRailRaptorModule;
import com.google.inject.Singleton;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.*;
import org.matsim.contrib.analysis.kai.KaiAnalysisListener;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastes;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup.Algotype;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup.ApproximationLevel;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy.OverwriteFileSetting;
import org.matsim.core.controler.listener.StartupListener;
import org.matsim.core.gbl.Gbl;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.algorithms.TransportModeNetworkFilter;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule.DefaultSelector;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.facilities.*;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.matsim.core.config.groups.PlanCalcScoreConfigGroup.ActivityParams;
import static org.matsim.core.config.groups.StrategyConfigGroup.StrategySettings;
import static org.matsim.core.config.groups.VspExperimentalConfigGroup.VspDefaultsCheckingLevel;

public class RunMatsimFromConfigFile {
	private static final Logger LOG = Logger.getLogger(RunMatsimFromConfigFile.class);

	public static void main(String[] args) {
		System.setProperty("matsim.preferLocalDtds", "true") ;

		// Config
		Config config = ConfigUtils.loadConfig(args[0]);
		double addShoppingProb = Double.parseDouble(args[1]);
		double addLeisureProb = Double.parseDouble(args[2]) + addShoppingProb;
		double addOtherProb = Double.parseDouble(args[3]) + addLeisureProb;

		// Activity adjustments
		config.planCalcScore().addActivityParams( new ActivityParams( "home" ).setTypicalDuration( 12.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "work" ).setTypicalDuration( 8.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "education" ).setTypicalDuration( 6.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "shopping" ).setTypicalDuration( 2.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "leisure" ).setTypicalDuration( 2.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "other" ).setTypicalDuration( 2.*3600. ) );

		// Scenario
		Scenario scenario = ScenarioUtils.loadScenario(config);

		// Network adjustments
		for(Link link : scenario.getNetwork().getLinks().values()){
			if (link.getLength() < 1. ) {
				LOG.warn("Link shorter than 1 meter");
				link.setLength( 1. );
			}
			if (link.getFreespeed() > 999999.) {
				LOG.warn("Free speed larger than 999999 m/s");
				link.setFreespeed( 999999. );
			}
		}

		// Plan adjustments
		PopulationFactory pf = scenario.getPopulation().getFactory();;
		for(Person person : scenario.getPopulation().getPersons().values()) {
			final List<PlanElement> planElements = person.getSelectedPlan().getPlanElements();
			// find home coordinate:
			Coord homeCoord = null ;
			for(PlanElement planElement : planElements){
				if (planElement instanceof Activity) {
					if (((Activity) planElement).getType().equals("home")) {
						homeCoord = ((Activity) planElement).getCoord();
						break ;
					}
				}
			}
			Gbl.assertNotNull( homeCoord );

			// remove last planElement if leg:
			if ( planElements.get( planElements.size()-1) instanceof Leg ) {
				planElements.remove( planElements.size()-1 ) ;
			}

			// bring people home if not at home; locachoice has problems if first/last act are not of "fixed" type
			Activity lastAct = (Activity) planElements.get( planElements.size()-1 );
			if ( !lastAct.getType().equals( "home" ) ) {
				lastAct.setEndTime( 24.*3600.-1 );
				Leg leg = pf.createLeg( "car" ) ;
				planElements.add(leg) ;
				Activity homeAct = pf.createActivityFromCoord( "home", homeCoord ) ;
				planElements.add(homeAct) ;
			}
		}

		// Facility adjustments
		Random rnd = MatsimRandom.getLocalInstance();;
		final ActivityFacilities facilities = scenario.getActivityFacilities();
		ActivityFacilitiesFactory ff = facilities.getFactory();;
		for( ActivityFacility facility : facilities.getFacilities().values()) {
			double random = rnd.nextDouble();
            if (random < addShoppingProb) {
				ActivityOption option = ff.createActivityOption("shopping");
				facility.addActivityOption(option);
            } else if (random < addLeisureProb) {
				ActivityOption option = ff.createActivityOption("leisure");
				facility.addActivityOption(option);
            } else if (random < addOtherProb) {
				ActivityOption option = ff.createActivityOption("other");
				facility.addActivityOption(option);
			}
		}

		// Controller
		Controler controler = new Controler(scenario);

		FrozenTastes.configure(controler);

		if (controler.getConfig().transit().isUseTransit()) {
			controler.addOverridingModule( new AbstractModule() {
				@Override
				public void install() {
					install(new SwissRailRaptorModule());
				}
			} );
		}

		controler.addOverridingModule( new AbstractModule(){
			@Override public void install(){
				addControlerListenerBinding().to( KaiAnalysisListener.class ).in( Singleton.class ) ;
				addControlerListenerBinding().toInstance( (StartupListener) event -> {
					NetworkUtils.writeNetwork( scenario.getNetwork(), config.controler().getOutputDirectory() + "/input_network.xml.gz" );
					new FacilitiesWriter( scenario.getActivityFacilities() ).write( config.controler().getOutputDirectory() + "/input_facilities.xml.gz" );
					// so I have this things in the output directory if the run crashes.  kai, jul'19
				} );
			}
		} ) ;

		controler.run();
	}
}