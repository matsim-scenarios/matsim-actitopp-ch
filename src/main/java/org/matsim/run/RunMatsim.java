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
import org.matsim.core.config.groups.PlanCalcScoreConfigGroup;
import org.matsim.core.config.groups.StrategyConfigGroup;
import org.matsim.core.config.groups.VspExperimentalConfigGroup;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy.OverwriteFileSetting;
import org.matsim.core.gbl.Gbl;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.network.algorithms.TransportModeNetworkFilter;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule.DefaultSelector;
import org.matsim.core.scenario.MutableScenario;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.facilities.ActivityFacilities;
import org.matsim.facilities.ActivityFacilitiesFactory;
import org.matsim.facilities.ActivityFacility;
import org.matsim.facilities.ActivityOption;

import java.util.*;

import static org.matsim.core.config.groups.PlanCalcScoreConfigGroup.*;
import static org.matsim.core.config.groups.StrategyConfigGroup.*;
import static org.matsim.core.config.groups.VspExperimentalConfigGroup.*;

/**
 * @author bcharlton
 *
 */
public class RunMatsim {
	private static final Logger log = Logger.getLogger( RunMatsim.class ) ;

	enum RunType { shortRun, medRun, longRun }

	public static void main(String[] args) {
		RunType runType = RunType.shortRun ;
		// yy not sure if it makes sense to keep the short/med/longRun differentiation at this level.  kai, jun'19

		Config config = ConfigUtils.loadConfig( "../../shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/config.xml" );

		config.network().setInputFile( "../transport_supply/switzerland_network.xml.gz" );
		config.plans().setInputFile( "population_1pct_plans_initial-coords.xml.gz" );
		config.facilities().setInputFile( "facilities_1pct.xml.gz" );

		config.global().setNumberOfThreads( 8 );

		switch( runType ) {
			case shortRun:
				config.controler().setLastIteration( 2 );
				config.controler().setWriteEventsUntilIteration( 2 );
				break;
			case medRun:
				config.controler().setLastIteration( 100 );
				break;
			case longRun:
				config.controler().setLastIteration( 1000 );
				break;
			default:
				throw new RuntimeException( Gbl.NOT_IMPLEMENTED) ;
		}

		// yyyy activity types need to come from Actitopp.  If we have different activity types for different durations, I prefer to program them (see in
		// matsim-berlin). kai, jun'19
		config.planCalcScore().addActivityParams( new ActivityParams( "home" ).setTypicalDuration( 12.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "work" ).setTypicalDuration( 8.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "education" ).setTypicalDuration( 6.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "shopping" ).setTypicalDuration( 2.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "leisure" ).setTypicalDuration( 2.*3600. ) );
		config.planCalcScore().addActivityParams( new ActivityParams( "other" ).setTypicalDuration( 2.*3600. ) );

		config.strategy().addStrategySettings( new StrategySettings( ).setStrategyName( FrozenTastes.LOCATION_CHOICE_PLAN_STRATEGY ).setWeight( 1.0 ).setDisableAfter( 10 ) );

		switch ( runType ){
			case shortRun:
				break;
			case medRun:
			case longRun:
				config.strategy().addStrategySettings( new StrategySettings().setStrategyName( FrozenTastes.LOCATION_CHOICE_PLAN_STRATEGY ).setWeight( 0.1 ) );
				config.strategy().addStrategySettings( new StrategySettings().setStrategyName( DefaultSelector.ChangeExpBeta ).setWeight( 1.0 ) );
				config.strategy().setFractionOfIterationsToDisableInnovation( 0.8 );
				config.planCalcScore().setFractionOfIterationsToStartScoreMSA( 0.8 );
			break ;
			default:
				throw new RuntimeException( Gbl.NOT_IMPLEMENTED ) ;
		}

		FrozenTastesConfigGroup dccg = ConfigUtils.addOrGetModule( config, FrozenTastesConfigGroup.class );;
		dccg.setEpsilonScaleFactors("1.0,1.0,1.0" );
		dccg.setAlgorithm( Algotype.bestResponse );
		dccg.setFlexibleTypes( "leisure,shopping,other" );
		dccg.setTravelTimeApproximationLevel( ApproximationLevel.localRouting );
		dccg.setRandomSeed( 2 );
		dccg.setDestinationSamplePercent( 10. );

		config.vspExperimental().setVspDefaultsCheckingLevel( VspDefaultsCheckingLevel.warn );

		// ### SCENARIO: ###

		Scenario scenario = ScenarioUtils.loadScenario( config ) ;

		// Create scenario
		TransportModeNetworkFilter filter = new TransportModeNetworkFilter(scenario.getNetwork());
		Network carNetwork = NetworkUtils.createNetwork();
		Set<String> modeSet = new HashSet<>();
		modeSet.add( TransportMode.car );
		filter.filter(carNetwork, modeSet);
		((MutableScenario)scenario).setNetwork(carNetwork );

		for( Link link : scenario.getNetwork().getLinks().values() ){
			if ( link.getLength() < 1. ) {
				log.warn("link shorter than 1 meter") ;
				link.setLength( 1. );
			}
			if ( link.getFreespeed() > 999999. ) {
				log.warn("free speed larger than 999999 m/s") ;
				link.setFreespeed( 999999. );
			}
		}

		new org.matsim.core.network.algorithms.NetworkCleaner().run(scenario.getNetwork());

		PopulationFactory pf = scenario.getPopulation().getFactory();;
		for( Person person : scenario.getPopulation().getPersons().values() ){
			final List<PlanElement> planElements = person.getSelectedPlan().getPlanElements();
			// find home coordinate:
			Coord homeCoord = null ;
			for( PlanElement planElement : planElements ){
				if ( planElement instanceof Activity ) {
					if ( ((Activity) planElement).getType().equals( "home" ) ) {
						homeCoord = ((Activity) planElement).getCoord() ;
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

		Random rnd = MatsimRandom.getLocalInstance();;
		final ActivityFacilities facilities = scenario.getActivityFacilities();
		ActivityFacilitiesFactory ff = facilities.getFactory();;
		for( ActivityFacility facility : facilities.getFacilities().values() ){
			double rrr = rnd.nextDouble();;
			if ( rrr < 0.33 ) {
				ActivityOption option = ff.createActivityOption( "shopping" ) ;
				facility.addActivityOption( option );
			} else if ( rrr < 0.66 ) {
				ActivityOption option = ff.createActivityOption( "leisure" ) ;
				facility.addActivityOption( option );
			} else {
				ActivityOption option = ff.createActivityOption( "other" ) ;
				facility.addActivityOption( option );
			}
		}

		// ### CONTROL(L)ER: ###

		Controler controler = new Controler(scenario);
		controler.getConfig().controler().setOverwriteFileSetting( OverwriteFileSetting.deleteDirectoryIfExists );

		FrozenTastes.configure( controler );

		controler.addOverridingModule( new AbstractModule(){
			@Override public void install(){
				addControlerListenerBinding().to( KaiAnalysisListener.class ).in( Singleton.class ) ;
			}
		} ) ;

		controler.run();

	}

}
