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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.NetworkFactory;
import org.matsim.api.core.v01.network.Node;
import org.matsim.api.core.v01.population.*;
import org.matsim.contrib.analysis.kai.KaiAnalysisListener;
import org.matsim.contrib.locationchoice.DestinationChoiceConfigGroup;
import org.matsim.contrib.locationchoice.frozenepsilons.BestReplyLocationChoicePlanStrategy;
import org.matsim.contrib.locationchoice.frozenepsilons.DCScoringFunctionFactory;
import org.matsim.contrib.locationchoice.frozenepsilons.DestinationChoiceContext;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.groups.PlanCalcScoreConfigGroup;
import org.matsim.core.config.groups.StrategyConfigGroup;
import org.matsim.core.controler.AbstractModule;
import org.matsim.core.controler.Controler;
import org.matsim.core.controler.OutputDirectoryHierarchy.OverwriteFileSetting;
import org.matsim.core.controler.events.ShutdownEvent;
import org.matsim.core.controler.listener.ShutdownListener;
import org.matsim.core.gbl.Gbl;
import org.matsim.core.population.PopulationUtils;
import org.matsim.core.replanning.strategies.DefaultPlanStrategiesModule;
import org.matsim.core.router.TripRouter;
import org.matsim.core.router.TripStructureUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordUtils;
import org.matsim.facilities.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.matsim.contrib.locationchoice.DestinationChoiceConfigGroup.Algotype.bestResponse;

/**
 * @author bcharlton
 *
 */
public class RunMatsim {
	private static final Logger log = Logger.getLogger( RunMatsim.class ) ;

	enum RunType { shortRun, medRun, longRun }
	// public MatsimTestUtils utils = new MatsimTestUtils() ;

	public static void main(String[] args) {
		// Gbl.assertIf(args.length >=1 && args[0] != "" );
		// run(ConfigUtils.loadConfig(args[0]));
		Gbl.assertIf(args.length == 0);
		run();
		// makes some sense to not modify the config here but in the run method to help with regression testing.
	}

	static void run() { // Config config) {
		RunType runType = RunType.shortRun ;
		Config config = ConfigUtils.createConfig() ;
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
		// TODO config.controler().setOutputDirectory( utils.getOutputDirectory() );
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "home" ) ;
			params.setTypicalDuration( 12.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			PlanCalcScoreConfigGroup.ActivityParams params = new PlanCalcScoreConfigGroup.ActivityParams( "shop" ) ;
			params.setTypicalDuration( 2.*3600. );
			config.planCalcScore().addActivityParams( params );
		}
		{
			StrategyConfigGroup.StrategySettings stratSets = new StrategyConfigGroup.StrategySettings( ) ;
			stratSets.setStrategyName( "MyLocationChoice" );
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
				stratSets.setStrategyName( "MyLocationChoice" );
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

		final DestinationChoiceConfigGroup dccg = ConfigUtils.addOrGetModule(config, DestinationChoiceConfigGroup.class ) ;
		switch( runType ) {
			case shortRun:
				dccg.setEpsilonScaleFactors("10.0" );
				break;
			case longRun:
			case medRun:
				dccg.setEpsilonScaleFactors("10.0" );
				break;
			default:
				throw new RuntimeException( Gbl.NOT_IMPLEMENTED ) ;
		}
		dccg.setAlgorithm( bestResponse );
		dccg.setFlexibleTypes( "shop" );
		dccg.setTravelTimeApproximationLevel( DestinationChoiceConfigGroup.ApproximationLevel.localRouting );
		dccg.setRandomSeed( 2 );
		dccg.setDestinationSamplePercent( 5. );
//		dccg.setInternalPlanDataStructure( DestinationChoiceConfigGroup.InternalPlanDataStructure.lcPlan );
		// using LCPlans does not, or no longer, work (throws a null pointer exception).  kai, mar'19

		// ---

		//		Scenario scenario = ScenarioUtils.loadScenario( config ) ;
		// (this will not load anything since there are no files defined)

		Scenario scenario = ScenarioUtils.createScenario( config ) ;
		// don't load anything

		NetworkFactory nf = scenario.getNetwork().getFactory();
		PopulationFactory pf = scenario.getPopulation().getFactory();
		ActivityFacilitiesFactory ff = scenario.getActivityFacilities().getFactory();

		// Construct a network and facilities along a line:
		// 0 --(0-1)-- 1 --(2-1)-- 2 -- ...
		// with a facility of same ID attached to each link.  Have home towards the left, and then select a shop facility, with frozen epsilons.

		Node prevNode;
		{
			Node node = nf.createNode( Id.createNodeId( 0 ) , new Coord( 0., 0. ) ) ;
			scenario.getNetwork().addNode( node );
			prevNode = node ;
		}
		for ( int ii=1 ; ii<1000 ; ii++ ) {
			Node node = nf.createNode( Id.createNodeId( ii ) , new Coord( ii*100, 0.) ) ;
			scenario.getNetwork().addNode( node );
			// ---
			addLinkAndFacility( scenario, nf, ff, prevNode, node );
			addLinkAndFacility( scenario, nf, ff, node, prevNode );
			// ---
			prevNode = node ;
		}
		// ===
		final Id<ActivityFacility> homeFacilityId = Id.create( "0-1", ActivityFacility.class ) ;
		final Id<ActivityFacility> initialShopFacilityId = Id.create( "1-2", ActivityFacility.class );
		for ( int jj=0 ; jj<1000 ; jj++ ){
			Person person = pf.createPerson( Id.createPersonId( jj ) );
			{
				scenario.getPopulation().addPerson( person );
				Plan plan = pf.createPlan();
				person.addPlan( plan );
				// ---
				Activity home = pf.createActivityFromActivityFacilityId( "home", homeFacilityId );
				home.setEndTime( 7. * 3600. );
				plan.addActivity( home );
				{
					Leg leg = pf.createLeg( "car" );
					leg.setDepartureTime( 7. * 3600. );
					leg.setTravelTime( 1800. );
					plan.addLeg( leg );
				}
				{
					Activity shop = pf.createActivityFromActivityFacilityId( "shop", initialShopFacilityId );
					// shop.setMaximumDuration( 3600. ); // does not work for locachoice: time computation is not able to deal with it.  yyyy replace by
					// more central code. kai, mar'19
					shop.setEndTime( 10. * 3600 );
					plan.addActivity( shop );
				}
				{
					Leg leg = pf.createLeg( "car" );
					leg.setDepartureTime( 10. * 3600. );
					leg.setTravelTime( 1800. );
					plan.addLeg( leg );
				}
				{
					Activity home2 = pf.createActivityFromActivityFacilityId( "home", homeFacilityId );
					PopulationUtils.copyFromTo( home, home2 );
					plan.addActivity( home2 );
				}
			}
		}

		final DestinationChoiceContext lcContext = new DestinationChoiceContext(scenario) ;
		scenario.addScenarioElement(DestinationChoiceContext.ELEMENT_NAME, lcContext);

		// CONTROL(L)ER:
		Controler controler = new Controler(scenario);
		controler.getConfig().controler().setOverwriteFileSetting( OverwriteFileSetting.deleteDirectoryIfExists );

		// set scoring function
		DCScoringFunctionFactory scoringFunctionFactory = new DCScoringFunctionFactory(controler.getScenario(), lcContext);
		scoringFunctionFactory.setUsingConfigParamsForScoring(true) ;
		controler.setScoringFunctionFactory(scoringFunctionFactory);

		// bind locachoice strategy (selected in localCreateConfig):
		controler.addOverridingModule(new AbstractModule() {
			@Override
			public void install() {
				addPlanStrategyBinding("MyLocationChoice").to( BestReplyLocationChoicePlanStrategy.class );
				addControlerListenerBinding().to( KaiAnalysisListener.class ).in( Singleton.class ) ;
			}
		});

		controler.addOverridingModule( new AbstractModule(){
			@Override
			public void install(){
				this.addControlerListenerBinding().toInstance( new ShutdownListener(){
					@Inject
					Population population ;
					@Inject
					ActivityFacilities facilities ;
					@Inject
					TripRouter tripRouter ;
					int binFromVal( double val ) {
						return (int) (val/10000.) ;
//						if ( val < 1. ) {
//							return 0 ;
//						}
//						return (int) ( Math.log(val)/Math.log(2) ) ;
					}
					@Override public void notifyShutdown( ShutdownEvent event ){
						switch( runType ) {
							case longRun:
							case medRun:
								return;
							case shortRun:
								break;
							default:
								throw new RuntimeException( Gbl.NOT_IMPLEMENTED ) ;
						}
						if ( event.isUnexpected() ) {
							return ;
						}
						double[] cnt = new double[1000] ;
						for( Person person : population.getPersons().values() ){
							List<TripStructureUtils.Trip> trips = TripStructureUtils.getTrips( person.getSelectedPlan(), tripRouter.getStageActivityTypes() );
							for( TripStructureUtils.Trip trip : trips ){
								Facility facFrom = FacilitiesUtils.toFacility( trip.getOriginActivity(), facilities );
								Facility facTo = FacilitiesUtils.toFacility( trip.getDestinationActivity(), facilities );
								double tripBeelineDistance = CoordUtils.calcEuclideanDistance( facFrom.getCoord(), facTo.getCoord() );
								int bin = binFromVal( tripBeelineDistance ) ;
								cnt[bin] ++ ;
							}
						}
						for ( int ii=0 ; ii<cnt.length ; ii++ ){
							if( cnt[ii] > 0 ){
								log.info( "bin=" + ii + "; cnt=" + cnt[ii] );
							}
						}
						// Note that the following "check" method is deliberately a bit imprecise (see implementation), since we are only interested in the
						// (approximate) distribution.  kai, mar'19
                            /* check( 684, cnt[0] );
                            check( 380, cnt[1] ) ;
                            check( 408, cnt[2] ) ;
                            check( 304, cnt[3] ) ;
                            check( 122, cnt[4] ) ;
                            check( 66, cnt[5] ) ;
                            check( 16, cnt[6] ) ;
                            check( 18, cnt[7] ) ;
                            check( 8, cnt[8] ) ;
                             */
					}

					// TODO Assert?
					//void check( double val, double actual ){
					//    Assert.assertEquals( val, actual, 2.*Math.max( 5, Math.sqrt( val ) ) );
					//}

				} );
			}
		} ) ;

		controler.run();

		// yyyy todo make test such that far-away activities have strongly lower proba
		// yyyy todo then make other test with other epsilon to show that average distance depends on this


/*
        // possibly modify config here

        // ---

        Scenario scenario = ScenarioUtils.loadScenario(config) ;

        // possibly modify scenario here

        // ---

        Controler matsim = new Controler( scenario ) ;

        // possibly modify controler here

        // ---

        matsim.run();
 */
	}

	public static void addLinkAndFacility( Scenario scenario, NetworkFactory nf, ActivityFacilitiesFactory ff, Node prevNode, Node node ){
		final String str = prevNode.getId() + "-" + node.getId();
		Link link = nf.createLink( Id.createLinkId( str ), prevNode, node ) ;
		Set<String> set = new HashSet<>() ;
		set.add("car" ) ;
		link.setAllowedModes( set ) ;
		link.setLength( CoordUtils.calcEuclideanDistance( prevNode.getCoord(), node.getCoord() ) );
		link.setCapacity( 3600. );
		link.setFreespeed( 50./3.6 );
		scenario.getNetwork().addLink( link );
		// ---
		ActivityFacility af = ff.createActivityFacility( Id.create( str, ActivityFacility.class ), link.getCoord(), link.getId() ) ;
		ActivityOption option = ff.createActivityOption( "shop" ) ;
		af.addActivityOption( option );
		scenario.getActivityFacilities().addActivityFacility( af );
	}


}
