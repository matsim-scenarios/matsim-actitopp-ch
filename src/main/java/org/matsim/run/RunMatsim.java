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
import org.matsim.api.core.v01.population.Activity;
import org.matsim.api.core.v01.population.Person;
import org.matsim.api.core.v01.population.PlanElement;
import org.matsim.contrib.analysis.kai.KaiAnalysisListener;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastes;
import org.matsim.contrib.locationchoice.frozenepsilons.FrozenTastesConfigGroup;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.PlanCalcScoreConfigGroup;
import org.matsim.core.config.groups.StrategyConfigGroup;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy.OverwriteFileSetting;
import org.matsim.core.gbl.Gbl;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.facilities.ActivityFacilities;
import org.matsim.facilities.ActivityFacilitiesFactory;
import org.matsim.facilities.ActivityFacility;
import org.matsim.facilities.ActivityOption;

import java.util.Random;

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

		Config config ;
//		if ( args==null || args[0]=="null" ){
			String configfile = "../../shared-svn/projects/snf-big-data/data/scenario/neuenburg_1pct/config.xml" ;
			config = ConfigUtils.loadConfig( configfile ) ;
//		} else {
//			config = ConfigUtils.loadConfig( args );
//		}

		config.network().setInputFile( "../transport_supply/switzerland_network.xml.gz" );
		config.plans().setInputFile( "population_1pct_plans_initial-coords.xml.gz" );
		config.facilities().setInputFile( "facilities_1pct.xml.gz" );

		switch( runType ) {
			case shortRun:
				config.controler().setLastIteration( 2 );
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

		// activity types need to come from Actitopp.  If we have different activity types for different durations, I prefer to program them (see in
		// matsim-berlin). kai, jun'19
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "home" ) ;
			params.setTypicalDuration( 12.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "work" ) ;
			params.setTypicalDuration( 8.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "education" ) ;
			params.setTypicalDuration( 6.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "shopping" ) ;
			params.setTypicalDuration( 2.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "leisure" ) ;
			params.setTypicalDuration( 2.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "other" ) ;
			params.setTypicalDuration( 2.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			StrategyConfigGroup.StrategySettings stratSets = new StrategyConfigGroup.StrategySettings( ) ;
			stratSets.setStrategyName( FrozenTastes.LOCATION_CHOICE_PLAN_STRATEGY );
			stratSets.setWeight( 1.0 );
			stratSets.setDisableAfter( 10 );
			config.strategy().addStrategySettings( stratSets );
		}
		switch ( runType ){
			case shortRun:
				break;
			case medRun:
			case longRun:{
				StrategyConfigGroup.StrategySettings stratSets = new StrategyConfigGroup.StrategySettings();
				stratSets.setStrategyName( FrozenTastes.LOCATION_CHOICE_PLAN_STRATEGY );
				stratSets.setWeight( 0.1 );
				config.strategy().addStrategySettings( stratSets );
			}
			{
				StrategyConfigGroup.StrategySettings stratSets = new StrategyConfigGroup.StrategySettings();
				stratSets.setStrategyName( DefaultPlanStrategiesModule.DefaultSelector.ChangeExpBeta );
				stratSets.setWeight( 1.0 );
				config.strategy().addStrategySettings( stratSets );
			}
			config.strategy().setFractionOfIterationsToDisableInnovation( 0.8 );
			config.planCalcScore().setFractionOfIterationsToStartScoreMSA( 0.8 );
			break ;
			default:
				throw new RuntimeException( Gbl.NOT_IMPLEMENTED ) ;
		}

		FrozenTastesConfigGroup dccg = ConfigUtils.addOrGetModule( config, FrozenTastesConfigGroup.class );;
		switch( runType ) {
			case shortRun:
				dccg.setEpsilonScaleFactors("10.0,10.0,10.0" );
				break;
			case longRun:
			case medRun:
				dccg.setEpsilonScaleFactors("10.0,10.0,10.0" );
				break;
			default:
				throw new RuntimeException( Gbl.NOT_IMPLEMENTED ) ;
		}
		dccg.setAlgorithm( FrozenTastesConfigGroup.Algotype.bestResponse );
		dccg.setFlexibleTypes( "leisure,shopping,other" );
		dccg.setTravelTimeApproximationLevel( FrozenTastesConfigGroup.ApproximationLevel.localRouting );
		dccg.setRandomSeed( 2 );
		dccg.setDestinationSamplePercent( 0.1 );

		// ### SCENARIO: ###

		Scenario scenario = ScenarioUtils.loadScenario( config ) ;

		for( Person person : scenario.getPopulation().getPersons().values() ){
			Coord homeCoord = null ;
			for( PlanElement planElement : person.getSelectedPlan().getPlanElements() ){
				if ( planElement instanceof Activity ) {
					if ( ((Activity) planElement).getType().equals( "home" ) ) {
						homeCoord = ((Activity) planElement).getCoord() ;
						break ;
					}
				}
			}
			Gbl.assertNotNull( homeCoord );
			for( PlanElement planElement : person.getSelectedPlan().getPlanElements() ){
				if ( planElement instanceof Activity ) {
					if ( ((Activity) planElement).getCoord()==null ) {
						((Activity) planElement).setCoord( homeCoord );
					}
				}
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
