import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.io.*;
import java.util.*;

public class getEventInfo {
  public static class Event {
    public Event(String word, long round, String country)
    {
      this.word = word;
      this.round = round;
      this.country = country;
    }
    public String word;
    public long round;
    public String country;
  }

  public static ArrayList<Event> eventArrayListUSA = new ArrayList<>();
  public static ArrayList<Event> eventArrayListCAN = new ArrayList<>();

  private static String Jun7CAN=" #EnviroWeek, #MTVAwardsStar, #NHLC2016, #Ramadan, #TheBachelorette, #BrockTurner, #PrinceDay, #SMWiTO, #Zootopia, #octranspo, #CTVUpfront, #BBCAN5, #GearsofWar4, #MAPLERESOLVE, #893fm, #BMOVolunteerDay, #tytlive, #ucalgarygrad2016, #WRD2016, #HHVan2016, #GPS16, #EXOLSelcaDay, #GoogleLife, #TakeASongToTheCasino, #EverGetTheFeelingThat, #MagNet16, #PrimaryDay, #PinnacleAwards, Dwane Casey, Kimbo Slice, Tax Freedom Day, Jack Gilinsky, NHL 17, Vente de RONA, Le PQ, US$50, Real Housewives of Toronto, U of C, PAX Prime, Caplansky's Deli, Guy Lafleur, Yonge and Eglinton, PAX West, Tiger Woods, University of Calgary, #TravelTuesday, #BBUK, #SweatshirtMusicVideo, #innobahn, #RowansLaw";
  private static String Jun6CAN="#loveONTfood, #MissUSA, #GameofThrones, #UFC200, #NHLC2016, #MusicMonday, #Ramadan, #DDay, #mondaymotivation, #SeniorsWeek, #HowToGetAPromotion, #UnREAL, #Sens, #rideauraccoon, #yegtraffic, #ctvhtt, #yegtheatre, #Colin, #NYAC2016, #FiddlesticksInThe6ix, #CISCAM16, #DolceAmoreSearchForAnswers, #WarcraftMovie, #LetMeIntroduceMyself, #MTVAwardsStar, #Tigers, #6fest, #blackoutday, Stanford, John Oliver, D-Day, FIFA 17, Tories, Kay Carter, Ted McMeekin, Axl Rose, Four Seasons Toronto, Dixie, Environment Canada, Phillippe Aumont, SPVM, Game 4, Fraser River, LaSalle, #BrockTurner, #onstorm, Don Cherry, #ExplainTwitterToA4YearOld, #TechTO, #RogersUpfront";
  private static String Jun5CAN="#PITvsSJS, #CanadiansThink, #bucciovertimechallenge, #OneDanceRemix, #lhjmq, #CopaAmerica, #SMMastery, #RTDNA2016, #yegpride, #FCM2016AC, #UFC199, #LSANOLA16, #DDay, #ShakespeareSunday, #CAEP16, #pridewpg, #ASRoma, #IM703Raleigh, #Monster, #RunForWomen, #CHEOTelethon, #SoccerAid2016, #Ramadan, #WPWalk, #JanewayTelethon, #BirminghamDL, #MyCAF, #AtTheFarmersMarket, #LilacFest, #Tourdelile, #CreateCoMtl, #GutsyWalk, Dominick Cruz, Sharks, Mohamed Ali, Joel Ward, Game 3, Brock Lesnar, Muhammad Ali, Sophie Gregoire Trudeau, Tom Mulcair, Jack Black, Niall and Louis, College and Spadina, Ariel Helwani, #5YearsOfTeenWolf, #FrenchOpenFinal, #RideForHeart, #RejectedNamesForHurricanes, #SundayFunday";
  private static String Jun4CAN="#hugyourcatday, #NationalCheeseDay, #BAREGYAL, #SLFLTallinn, #Westfest2016, #CAEP16, #RTDNA2016, #MakeATVShowHigh, #FieldTripTO, #macandcheesefest, #WizardWorldPhilly, #Moncton, #brunch, #fvcagm, #DoorsOpenOttawa, #BoxAFilm, #tdsbrun, #wchamont, #lemonade2016, #100in1day, #ScrambleAMovieTitle, #CommitACrimeIn5Words, Ukrainian, Titan, Listeria, Slam, Le Drakkar, Charlottetown, Blainville-Boisbriand, Tigres, Mohamed Ali, Kevin Mandolese, Xavier Bernard, Alexis Gravel, Andrew Coxhead, Anderson MacDonald, Shawn Element, Evan MacKinnon, Marc-Olivier Alain, Cédric Desruisseaux, #NHLCombine, #SMMastery, Shane Bowers, Garbine Muguruza, #QDraft, #yegpride, #RIPMuhammadAli, #yegbeerfest, #NewMusicFriday, #MakeMeAnOutcastIn5words";
  private static String Jun3CAN="#RBCBlueWater, #TimBosma, #OrphanBlack, #SummerSurvivalTips, #PrivateEyes, #ACA2016, #MRIA16, #GrassyNarrows, #Habs, #musicmatters, #ThirstyThursday, #SimySkin, #Prince, #PrideMonth, #MTLACTION2016, #justmojis, #WorldEatingDisordersDay, #VisitCZ, #ALLin216, #PowerBallTO, #hackedbyseb, #PositivityIn5Words, #Religious80sSongs, #MyWeekIn4Words, #FCM2016AC, Muhammad Ali, Kyrie, Ezra Levant, Delly, Liz Howard, Marc Leduc, John Legend, UCLA, Game 2, JR Smith, Paul Ryan, Steph and Klay, YANNICK NÉZET-SÉGUIN, Elon Musk, Panthers, Parliament Hill, Frank Ocean, #uwott, #TEDxWpg2016, #GIRLSTALKBOYS, Canada AM, #LoveIsLove, Shaun Livingston, West Hollywood, #BetterOniTunes";
  private static String Jun2CAN="#cswa2016, #MRIA16, #IAlwaysRollMyEyesWhen, #ArmenianGenocide, #DMFSToronto, #exrdi, #consent2016, #BatmanvSuperman, #Wave2016, #RBCBlueWater, #HaltonFutures, #ICouldBeVPBecause, #EULCS, #WorldEatingDisordersDay, #ItalianRepublicDay, #electoralreform, #dairyrally, #HarlequinMoments, #ThursdayThoughts, #dayofcaring, #musicmatters, #1000Women, Kingsway, Schlatman, Radiothon, Hospital, Orlando, Music Director, Minnesota, Timberlea, Leo Hayes, Elon Musk, Big City Mayors, Leitch, Rexdale, Michalski, Jacques Daoust, Noudga, Serena Williams, Tom Flanagan, Premier Tech, Scott Long, Procès Cinar, #FCM2016AC, #BetterAtMidnight, #MyWeekIn4Words, #TEDxWpg2016, Dwane Casey, #LoveIsLove, Canada AM";
  private static String Jun1CAN="#seniorsmonth, #skbudget, #UPAC, #CBCNN, #NHLJets, #wellbeing, #cgsa2016, #MarilynMonroe, #June1st, #RapeCulture, #1lineWed, #APTNTrudeau, #WhenMyStomachGrowls, #SaySomethingNiceDay, #InternationalChildrensDay, #hkccrexdale, #thisisdailyhive, #ymmhome, #MyUnOlympicEventWouldBe, #DignityRoundtable, #StartupChats, #WorldMilkDay, #SLFLStockholm, #ReliefLine, #NCBD, #MayorsDayOut, #ymmstrong, #CLAOtt16, #WhenIWasYoung, #PrideMonth, #ciraif, #HeatUpAMovie, #GlobalRunningDay, Gasquet, Welland, Carter, Plaxton, Parks Canada, W-18, Apple Pay, NOW Toronto, Dungey, Swirl Face, Jimmy Paredes, #JasperCIN, #ownthethrone, #VisitCZ, #WednesdayWisdom, #CampDay, UCLA";
  private static String May31CAN="#WPRF2016, #congressh, #AECanada, #OKCvsGSW, #MemorialDay2016, #StanleyCupFinal, #TheBachelorette, #ldnlab, #caregivers, #SupplyManagement, #Shawn4MMVA, #NHLJets, #MUNgrad16, #Bombardier, #GetMotivated, #10DaysTilBeSomebodyFilm, #2016LUL, #GeorgeinGibsons, #TheHills, #MPChickenHero, #Blackfish, #NEXTGENre, #SLFLOslo, #TuesdayMotivation, #CAMA2016, #PrideMonth, #CCFI16, #ItsWorthLivingFor, Heather Crowe, Unity, Villeray, Jerry Brown, Ulf Samuelsson, Labeaume, North Korean, Monsef, Marc Labrèche, Dr. Verna Yiu, Speaker, Rwanda, Maxime Bernier, Jay Z, Scott Long, Jose Bautista, #3DaysTilBetter, Smich, #BadSummerCampNames, #WorldNoTobaccoDay, #MyBestFriendIn4Words, #TravelTuesday";
  private static String May30CAN="#ThankYouForYourService, #NPcrrlg, #SSHRCStorytellers, #AllerGen2016, #BecauseItsTheCup, #JonahsTwitterMeetup, #SummerStartsNow, #pycon2016, #BlanketExercise, #sfMacbeth, #TBEXinMN, #ReconciliatiON, #APLA2016, #electoralreform, #YorkU, #qgrad16, #igniteliteracy, #5H727, #MeatlessMonday, #CincinnatiZoo, #csdhschn16, #bigthinking, #biketoworkweek, #Harambe, #TourismWeek, #MusicMonday, #YouthInOffice, #DORAS2016, #FRACAM, #AECanada, #ncaaLAX, Halsey, Vaughan, DeMar, Zika, Fildebrandt, Detroit River, Paredes, Bismack Biyombo, Ed Martin, Adonis Stevenson, Board of Health, Lise Payette, Giroud, The Big Slice, #HowToGetReadyForADate, #4DaysTilBetter, #mondaymotivation, #RejectedBlogTopics, #MemorialDay2016";
  private static String May29CAN="#MakeAMaleFemaleAndViceVersa, #ChickenTrump, #IfKidsWereCops, #TheStorytellerTour, #caturday, #weekend, #polyglotconf, #OKCvGSW, #1party, #LeoAwards16, #XMenApocalypse, #ReasonsDatesFail, #YouMightBeAFreak, #MemorialDayWeekend, #WWEWinnipeg, #cbas2016, #VANvHOU, #ChampionsLeague, #nationalhamburgerday, #RedSox, #MTLvLA, #TFCLive, #TEDxStanleyPark, #HalaMadrid, #CAJ16, #cpc16, #WPRF2016, #WhatIDoWhenYoureNotAround, #Clermont7s, #maplesyrupedu, Alicia Keys, Chase Utley, Torres, Russell Martin, Durant, Johnny Depp, Steven Adams, Atletico, Ronaldo, Klay Thompson, Mediterranean, #UCIMTBWC, #SundayFunday, #RunCalgary, Ramos-Vinolas, #runottawa2016, #MonacoGP, #gafesummit, #nationalhamburgerday, #runottawa2016";
  private static String May28CAN="#d2l2016, #SLFLHerning, #hcm2016, #ManilaMajor, #welcomerefugees, #SkinnyTVShows, #DaydreamTour, #RMAATL, #polyglotconf, #13DaysTilBeSomebodyFilm, #TakeASongOutToEat, #nationalhamburgerday, #ReasonsDatesFail, #wcyyc, #dot16, #congressh, #FamilyFirst, #ImNotGettingUp, #RedSox, Gilles Bouchard, Joey Bats, Novak Djokovic, San Siro, CHL Player of the Year, Benzema, Aaron Loup, Gibby, Gavin Floyd, Griezmann, Ortiz, Stroman, Alicia Keys, Torres, Bob Rae, Kimbrel, Ramos, #TEDxStanleyPark, Danilo, Atletico, Real Madrid, #CBCMusicFest, #championsleaguefinal2016, #OurMoment, #uclfinal, #flashbackfriday, #halicop, #redsox, #wch2016, #fairytalenewsheadlines, #memorialdayweekend";
  private static String May27CAN="#727OutNow, #ProjectClaudia, #cpc16, #wpg2016, #ThankYouStephenHarper, #GVBOT, #NNA2015, #UCLfinal, #ottawaraceweekend, #TOWebFest, #ENGvAUS, #friyay, #SCCC, #mact2016, #convocation2016, #MemorialDayWeekend, #WCH2016, #AnimeNorth, #Bute, #abstorm, #FlashbackFriday, #ChickenTrump, #CanMNT, #KeepAFilmSafe, #7DaysTilBetter, Queensway, Vaughan, Bath Rd, Hwy 7, Kevin O'Leary, Brad Trost, WB Gardiner, Rizzuto, Environment Canada, Maple Batalia, Heimlich, Wynne, Rona Ambrose, John McEnroe, Dustin Brown, Bautista, Marcus Rashford, Yonge, Rio Olympics, #HaliCop, #FridayFeeling, #SpaceX, #CAJ16, #FairytaleNewsHeadlines, Johnny Depp";
  private static String May26CAN="#YWCAwoda, #CHSLD, #LMRENT, #Wheat, #15DaysTilBeSomebodyFilm, #UFC199, #ChardonnayDay, #reconciliation, #ThirstyThursday, #OSMBal, #LPC16, #AngelMuse, #Game7, #ThingsOfNoUseToMe, #PutButtsInAVideoGameTitle, #PubPD, #Bruyere, #FireAid4YMM, #BAREGYAL, #OSWCityFare, #BadInspirationalQuotes, #yesgirlmontreal, #HBxTasteCanada, #KevinVickers, Michael Phair, Kendall, Jean-Béliveau, Keystone XL, Guwop, Finch, Yanks, Oracle, Stéfanie Trudeau, Chapman, Matricule 728, Tom Nilsson, Devon Travis, All-NBA, A-Rod, Happ, Stamkos, #ComedianASong, #cjfjtalk, #ProjectClaudia, Jon Cooper, Lucian Bute, #DadAdvicein3Words, #wpg2016, Gucci, Troy Ave";

  private static String Jun7USA="#StrikeOutCancer, #NAHLDraft, #GlamourAwards, #NextSG, #ReinoUnidoConSonora, #thefive, #SMWLA, #FavCrawfVine, #insideDTP, #TrailheaDX, #SIS16, #FilmsPlayingGames, #ewgc, #AlexPeepsAreAwesome, #USAvCRC, #SweatshirtMusicVideo, #MyBumperSticker, #LagosToLondon, #IncomingFreshmenAdvice, #TravelTuesday, #ITVEURef, #EXOLSelcaDay, #HPEDiscover, Muhammad Ali Way, Jordy Mercer, Clarke Schmidt, Aubameyang, Jared Poche, Meryl Streep, Jim Koch, Valerie Jarrett, Theresa Saldana, Live Photos, PLAY INESPERADO ON SPOTIFY, Ana Cacho, Albert Almora, Aubrey McClendon, Qaddafi, Roger Goodell, I Voted, John Jones, Niese, Mark Kirk, Tiger Woods, PAX West, #MakeYourKidsEventTolerable, #BBUK, #TakeASongToTheCasino, #BernieHillaryDuets, #PrimaryDay" ;
  private static String Jun6USA="#3686South, #StateOfWomen, #social16, #SelfieForJohnny, #FCNStorm, #TheFive, #lammys, #GetThanked, #BobayGirls, #OperaWednesdays, #AngieTribeca, #MakesHillaryEmotional, #CancerMoonshot, #notaprison, #ham4pamphlet, #SparkBizApps, #Supergirl, #HowToGetAPromotion, #PANvBOL, #MusicMonday, #ARMYSelcaDay, #SpecialReport, #CFDAAwards, #TSColin, Nazzi, Battle of the Block, Von Miller, Khalil, Stitcher, Mayor Strickland, Cassandra Butts, FIFA 17, Joebarly Feat, Shaq Lawson, Bob Odenkirk, Katrina Pierson, Roger Clinton, Pacific Rim 2, Fred Sheheen, Highlands Ranch, Kurt Busch, Gus Malzahn, Alexander Camelton, Prince Day, Grim Sleeper, #MTVAwardsStar, #LetMeIntroduceMyself, #BrockTurner, #blackoutday, #ExplainTwitterToA4YearOld" ;
  private static String Jun5USA="#klovefanawards, #FakeStanleyCupFacts, #BigAssPoolParty, #OnAssignment, #BrockTurner, #5DaysTilBeSomebodyFilm, #panpride, #Primarias2016, #LaLigaxRCN, #rwchat, #RespectChrisEvans, #60Minutes, #IndyCar, #MoviesAtTheBakers, #PuertoRicoPrimary, #Ramadan, #SundayFunday, #CanadianBirthdayTraditions, Seth Beer, Zach Britton, Marco Estrada, Drew Harrington, CONCACAF, Vamos Venezuela, Severe Thunderstorm Warning, I Can't Go Back, Matt Wieters, Katrina Pierson, Rafa Marquez, Super Regional, Carlee Wallace, Jake Arrieta, Millonarios, William McGirt, Corey Seager, Tropical Storm Colin, Aqib Talib, #MEXvURU, Game 2, David Gilkey, #SoccerAid2016, #JAMvVEN, #ThatWasMyCueToLeave, #MissUSA, #ArsenicAndOldLace, #HackLearning, #ashtonappreciationday, #ThatShouldHaveAName, #NOISundays, #spiritchat" ;
  private static String Jun4USA="#csuftcs, #Justice16, #NHLCombine, #VCPoloClassic, #WCAVL, #killscreenfest, #FelizSabado, #AFWF16, #RootsPicnic, #buywildlifeonitunes, #GERHUN, #SpreadLoveWithCamila, #infoseccosmo, #Demvention, #edcampmagic, #PoconoGreen250, #LetsMovie, #satchatwc, #hugyourcatday, #GetWellSoonSeokjin, #hackforchange, #caturday, #MLPSeason6, #WizardWorldPhilly, #5YearsOfPTX, #4OUNJ, #ForAGoodTimeI, #BoxAFilm, Gene Cone, Joe Frazier, Shay Smiddy, Sea the Stars, Rumble in the Jungle, Mark Teixeira, Mary Carillo, George Foreman, Blackness, Freddie Gibbs, Garbiñe, Jerry Izenberg, #ReasonRally, Cassius Clay, #ScrambleAMovieTitle, #NationalCheeseDay, #CommitACrimeIn5Words, Serena, #RIPMuhammadAli, Music Core, Rumble in the Jungle, #PowerhouseLA" ;
  private static String Jun3USA="#NiUnaMenos, #Justice16, #BuenViernes, #LesserFightClubRules, #HereToHelp, #MendesAtZ100, #Migrahack, #GovBallNYC, #FitnessFriday, #HITsm, #VenusSilkExpert, #friyay, #HackForChange, #OMCchat, #FunFactFriday, #equitymatters, #summarizeSWEU, #NewMusicFriday, #USAvCOL, #MyOnePhoneCallGoesTo, #DetroitGP, #WalmartShares, #bipride, #StarsInTheAlley, #TravelSkills, #ThingsNotToDoAtTheZoo, #JobsReport, #FlashbackFriday, Scholars Say, Tommy Haas, Too Short, Trea Turner, Wawrinka, Luis Salom, Toni Braxton, Fed's Brainard, Jeff Hornacek, Matt Damon, Quavo, Jarrett Allen, Dick Morris, Donny Everett, Khaled, #ILoveTaylorSwiftBecause, #FridayReads, #MakeMeAnOutcastIn5words, #Justmoji, Cruel Winter, For Free, #NationalDonutDay" ;
  private static String Jun2USA="#SidneyCrosbyCheats, #PokemonSunMoon, #bringonthefuture, #GoogleDoodle, #GamerBooks, #SummerSweepstakes, #Heart2Heart, #TeamNouis, #IWishOtherParentsWould, #ImMissingYouBecause, #NGNDay, #seochat, #NationalDonutDay, #SplendaSavvies, #MyCatAlwaysSays, #MedX, #UberDonas, #HarlequinMoments, #RestoreTheVRA, #EULCS, #TwitterSmarter, #KeepClinicsOpen, #TravelTopics, #ICouldBeVPBecause, #NBAFinalsVote, #IAlwaysRollMyEyesWhen, Sean Rad, Jordan Cameron, Dani Alves, Bad Vibrations, Armenian, Matthew Slater, Air Force Academy, Kiki Bertens, Putintseva, MLG Orlando, The Sims, Brooklyn Park, Dwane Casey, Prince Died of Opioid Overdose, Kill List, Serena, Mainak Sarkar, Stacey Dash, Madison Bumgarner, #BetterAtMidnight, #MyWeekIn4Words, #ThursdayThoughts, #WearOrange, #LoveIsLove" ;
  private static String Jun1USA="#NCBD, #halfnakedhouseparty, #PawsToSavor, #WorldOutlanderDay, #NYRenews, #bufferchat, #winniesun, #HurricaneSeason, #WishIKnewHowToQuit, #TeamLouis, #WhenMyStomachGrowls, #3YearsOfTryHard, #ksleg, #TrumpUniversity, #ExpediaChat, #ChatSnap, #CFAOne, #Q102ShawnNYC, #popentjacob, #VisitCZ, #activeshooter, #MPC16, #EndAlzheimers, #IAmAnImmigrant, #PrideMonth, #MyUnOlympicEventWouldBe, Elizabeth Holmes, Ginni Rometty, Kenneth Starr, College Football Hall of Fame, Hue Jackson, nazr mohammed, PGA Tour, fbi and atf, Cory Booker, nike air pegasus, 92 modern makeover, universidad de california, Monster Trucks, Morgan Freeman, Westwood, Marlon Byrd, #JamarClark, Mary Meeker, Marilyn Monroe, #HeatUpAMovie, #WhenIWasYoung, #WednesdayWisdom, #GlobalRunningDay, UCLA" ;
  private static String May31USA="#ConMaduroATodaMarcha, #DiorCruise, #bizheroes, #10DaysTilBeSomebodyFilm, #WhatILearnedToday, #MPC16, #NYCMenTeach, #AmenazoComoCarlosJoaquin, #TriviaTuesday, #FilmsEatingSweets, #GoRedGetFit, #Rate, #NASASocial, #TheHills, #StopBDS, #ThisMaySoundStrangeBut, #ONC2016, #3DaysTilBetter, #PlayHouseAtlanta, Walt Whitman, Pro Bowl, Fizz, I Stand With Gawker, Love & Hip Hop ATL, North Korean, Fandom is Broken, Mike Francesa, Skai Moore, Kevin Dodd, Walter Thurmond, Butch Jones, My Side, Carta Democrática, Bill O'Brien, Nick Saban, Drug Dealers Anonymous, Rogue One, Jan Crouch, Uncle Verne, Hugh Freeze, Pusha T, Michael Jace, Jerry Brown, Drew Peterson, #TrumpPressConference, Verne Lundquist, #ItsWorthLivingFor, #HackedByJohnson, #BadSummerCampNames, #TuesdayMotivation" ;
  private static String May30USA="#Harvey, #Hatch, #ElGorgojoDeMORENA, #MotivationMonday, #policechase, #OccupationalMusic, #FRACAM, #TheGreatEscape, #ATLMansionParty, #JonahsTwitterMeetup, #justiceforharambe, #RoadToOmaha, #SummerStartsNow, #frantasticmonday, #RuinMyDayOffIn4Words, #ClintonPasswords, #pursuit, #pycon2016, #GettingToKnowWhitesiders, #11DaysTilBeSomebodyFilm, #ThankYouForYourService, #5H727, Florida Evans, Thane Maynard, Eric Holder, Arlington National Cemetery, J.B. Shuck, Bernlohr, Mallex Smith, Camden Yards, Folty, Bogaerts, Mitch Gaspard, Melky Cabrera, NCAA Tournament, Steven Wright, Jose Quintana, Malik Newman, Tyler Wilson, Big Boi, Doc Savage, Addison Reed, Augie Garrido, Smart House, #HowToGetReadyForADate, #RejectedBlogTopics, #4DaysTilBetter, Jim Grobe, #ncaaLAX, #MemorialDay2016" ;
  private static String May29USA="#MyOtherTwitterAccount, #RIPMimaw, #RipGirls, #OTHERtone, #TotalEclipseOfGaza, #HoranandRose, #5DaysTilBetter, #Soundset2016, #RCBvSRH, #rdconcurso, #IncompetentXMen, #TylerOakleyTwitterMeetup, #12DaysTilBeSomebodyFilm, #WhenIMissWork, #RollingThunder, #CocaCola600, #SundayFunday, Austin Jackson, Rice 1, Tufts, Nick Banks, Double Teamed, Broxton, Andrew Miller, Federal Racketeering Charges, Heath Hembree, Harris English, Darren O'Day, Eovaldi, Jayson Werth, Bill Weld, Tarzan, Starlin Castro, Gary Johnson, Harambe, #TopGear, #MyPastExperiencesTaughtMe, #FakePartyFacts, #BadDrinkNames, Gorilla, #Indy500, #Comrades2016, #SaturdayNightConfessions, #LyddenRX, #ENGvSL, #CincinnatiZoo, #rnrliverpool, #marr, #gorilla, #UNNGO2016" ;
  private static String May28USA="#ICNA2016, #xicanitx, #PreClassic, #Stream727, #DaydreamTour, #13DaysTilBeSomebodyFilm, #rickychat, #ncaaLAX, #ImNotGettingUp, Billy Butler, Hull City, James Shields, Barney Frank, Kody Clemens, Ubaldo, Marcelo, Bob Dole, Andrea Bocelli, Pineda, James Loney, Kroos, Hendricks, Kimbrel, Tropical Storm Bonnie, Alicia Keys, #ReasonsDatesFail, #Hisense300, #TakeASongOutToEat, Zidane, #nationalhamburgerday, Ronaldo, Bryce Dejean-Jones, #uclfinal, #28mayo, #nassh2016, #davidrosemdc, #dont___shameme, #whatuwearin, #mcmldn16, #svlonabscbn, #udgrad2016, #twittergodfather, #mudcontiranosnosedialoga, #birminghampride, #bluehensforever, #500parade, #aporlaunadecima, #felizsabado, #breezysays, #voicekids3ph" ;
  private static String May27USA="#14DaysTilBeSomebodyFilm, #VZWBuzz, #VictorianEraModernTimes, #KCCOSalute, #Hellerball, #questionsforpup, #JumpIn, #5H727, #WeAreWithYouAmberHeard, #BottleRock, #GoldenGateBridge, #HarryPotterWeekend, #THAICOM8, #100DCOMs, #727ListeningParty, #CarbDay, #BRTalk, #HillaryArrestQuotes, #ENGvAUS, #FlashbackFriday, #7DaysTilBetter, #ImWithAmber, #FairytaleNewsHeadlines, Kyle Wright, Dustin Brown, Miles Morales, Wendy Wu, Tom Hiddleston, Money Go, Wisconsin Idea, Wayne Rooney, First Day Out Tha Feds, Ricky Williams, Eric Dier, Cassandra Butts, Will Craig, Andrew Bynum, Adam Morgan, Megan Good, Mark Salling, Dr. Heimlich, David Ross, Tropical Depression, Lemonade Mouth, #FridayFeeling, #SDTrumpRally, #KeepAFilmSafe, #MakeMeMadIn5Words, Johnny Depp, #MemorialDayWeekend" ;
  private static String May26USA="#selfiefornash, #GHOpen, #Hisense300, #AmberAlert, #FrenchASong, #kcwx, #ThirstyThursday, #ReinventCA, #NCAATF, #DangerousWomanOnSpotify, #MaduroEsColombiano, #LuckyToHaveNiall, #15DaysTilBeSomebodyFilm, #FocusForJacob, #THAICOM8, #BadInspirationalQuotes, #ThingsOfNoUseToMe, Mega Man, Nathaniel Kibby, Tornado Warning, Plaxico Burress, Rubio, Neil Allen, Moose, CC Sabathia, Keyshia, Cheddar Bob, Ray Rice, Mike Moustakas, Hector Olivera, Anthony Davis, Horse Cave, #bbcqt, Boosie, James Harden, #blacksalonproblems, #PutButtsInAVideoGameTitle, Baylor, #ComedianASong, All-NBA, Mike D'Antoni, #DadAdvicein3Words, Troy Ave, Gucci, #26May, #StevieNicks, #MonacoGP, #VzlaYEstudiantesALasCalles, #LaFormaMasFacilDeLigar, #GivetoLincoln" ;

  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    InputStream inputStream = TopologyHelper.class.getClassLoader().getResourceAsStream( "config.properties" );
    properties.load( inputStream );
    inputStream.close();
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String COUNTS_TABLE = properties.getProperty("counts.table");
    String EVENTS_TABLE = properties.getProperty("events.table");

    CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE);
    ArrayList<Long> roundlist = new ArrayList<>();
    ResultSet resultSet;
    try {
      resultSet = cassandraDao.getRoundsFromEvents();

      Iterator<Row> iterator = resultSet.iterator();
      while(iterator.hasNext())
      {
        Row row = iterator.next();
        roundlist.add(row.getLong("round"));
      }
      Collections.sort(roundlist, new Comparator<Long>() {
        public int compare(Long m1, Long m2) {
          return m1.compareTo(m2);
        }
      });


    } catch (Exception e) {
      e.printStackTrace();
    }

    for(long r:roundlist)
    {
      writeInfo(cassandraDao,r,"USA");
      writeInfo(cassandraDao,r,"CAN");
    }

    for(Event eventUSA:eventArrayListUSA)
    {
      for(Event eventCAN:eventArrayListCAN)
      {
        if(eventCAN.word.equals(eventUSA.word))
        {
          writeToFile("same", "events", "Event: " + eventCAN.word +
                  ", Canada timestamp: " + new Date(12*60*1000*eventCAN.round) +
                  ", America timestamp: " + new Date(12*60*1000*eventUSA.round));
        }
      }
    }

    for(Event eventUSA:eventArrayListUSA) {
      if(match(Jun1USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 1 June." );
      }
      else if(match(Jun2USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 2 June." );
      }
      else if(match(Jun3USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 3 June." );
      }
      else if(match(Jun4USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 4 June." );
      }
      else if(match(Jun5USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 5 June." );
      }
      else if(match(Jun6USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 6 June." );
      }
      else if(match(Jun7USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 7 June." );
      }
      else if(match(May26USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 26 May." );
      }
      else if(match(May27USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 27 May." );
      }
      else if(match(May28USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 28 May." );
      }
      else if(match(May29USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 29 May." );
      }
      else if(match(May30USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 30 May." );
      }
      else if(match(May31USA, eventUSA.word)){
        System.out.println("USA Match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round) + ", twitter date: 31 May." );
      }
      else
      {
        System.out.println("USA No match: " + eventUSA.word + ", event timestamp: " + new Date(12*60*1000*eventUSA.round)  );
      }
    }

    for(Event eventCAN:eventArrayListCAN) {
      if(match(Jun1CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 1 June." );
      }
      else if(match(Jun2CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 2 June." );
      }
      else if(match(Jun3CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 3 June." );
      }
      else if(match(Jun4CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 4 June." );
      }
      else if(match(Jun5CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 5 June." );
      }
      else if(match(Jun6CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 6 June." );
      }
      else if(match(Jun7CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 7 June." );
      }
      else if(match(May26CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 26 May." );
      }
      else if(match(May27CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 27 May." );
      }
      else if(match(May28CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 28 May." );
      }
      else if(match(May29CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 29 May." );
      }
      else if(match(May30CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 30 May." );
      }
      else if(match(May31CAN, eventCAN.word)){
        System.out.println("CAN Match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round) + ", twitter date: 31 May." );
      }
      else
      {
        System.out.println("CAN No match: " + eventCAN.word + ", event timestamp: " + new Date(12*60*1000*eventCAN.round)  );
      }
    }

    return ;
  }

  private static boolean match(String list, String word)
  {
    List<String> stringList = new ArrayList<>(Arrays.asList(list.split(",")));
    for(String s:stringList)
    {
      if(s.equals(word) || s.equals("#"+word) ||  word.equals("#"+s)) return true;
    }
    return  false;
  }
  private static void writeInfo(CassandraDao cassandraDao, long r, String country) throws Exception {
    ResultSet rsCAN = cassandraDao.getFromEvents(r,country);
    Iterator<Row> iteratorCAN = rsCAN.iterator();
    while (iteratorCAN.hasNext())
    {
      Row row = iteratorCAN.next();
      String word = row.getString("word");
      double incrementpercent = row.getDouble("incrementpercent");
      Date d = new Date(12*60*1000*r) ;

      Event event = new Event(word,r,country);
      if(country.equals("USA"))
        eventArrayListUSA.add(event);
      else
        eventArrayListCAN.add(event);

      if(incrementpercent<5)
      {
        writeToFile(country, "5-10_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=5 && incrementpercent<10)
      {
        writeToFile(country, "5-10_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=10 && incrementpercent<20)
      {
        writeToFile(country, "10-20_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=20 && incrementpercent<30)
      {
        writeToFile(country, "20-30_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=30 && incrementpercent<40)
      {
        writeToFile(country, "30-40_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=40 && incrementpercent<50)
      {
        writeToFile(country, "40-50_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else
      {
        writeToFile(country, "50+_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      System.out.println(r + " " + d + " " + row.getString("country") + " " + word );
    }
  }

  public static void writeToFile(String country, String fileName, String line)
  {
    try {
      PrintWriter writer = new PrintWriter(new FileOutputStream(
              new File("/home/ceren/Desktop/report/" + country + "_" + fileName + ".txt"),
              true /* append = true */));
      writer.println(line);
      writer.close();

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
