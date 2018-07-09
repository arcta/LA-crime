/* filter data for local applications */
delete from crime_dictionary where term in ('ii','iii','fel','etc','veh','via','non','agg','aggrav','adw','mkt','oz','iv','pos','res','gta','alc','poss','less','grand');
delete from crime_dictionary where word in ('yrs','years');
delete from crime_dictionary where word like 'auto%';
delete from crime_dictionary where term in 
('offens','feloni','misdemeanor','misd','miscellan','misc','without','crimin','crime','found','commit','true','law','re');

delete from crime_wordnet where term1 in ('ii','iii','fel','etc','veh','via','non','agg','aggrav','adw','mkt','oz','iv','pos','res','gta','alc','poss','less','grand');
delete from crime_wordnet where term2 in ('ii','iii','fel','etc','veh','via','non','agg','aggrav','adw','mkt','oz','iv','pos','res','gta','alc','poss','less','grand');
delete from crime_wordnet where term1 in 
('offens','feloni','misdemeanor','misd','miscellan','misc','without','crimin','crime','found','commit','true','law','re');
delete from crime_wordnet where term2 in 
('offens','feloni','misdemeanor','misd','miscellan','misc','without','crimin','crime','found','commit','true','law','re');
delete from crime_wordnet where word1 in ('yrs','years') or word2 in ('yrs','years');
delete from crime_wordnet where word1 like 'auto%' or word2 like 'auto%';

update crime_taxonomy set criminal = 0 where description like '%non-criminal%';
update crime_taxonomy set criminal = 0 where description like '%accident%';
update crime_taxonomy set criminal = 0 where description like '%suicide%';
update crime_taxonomy set criminal = 0 where description like '%person dead%';
update crime_taxonomy set criminal = 0 where description like '%drunk%';
update crime_taxonomy set criminal = 0 where description like '%disorderly%';
update crime_taxonomy set criminal = 0 where description like '%misd%';
update crime_taxonomy set criminal = 1 where description like '%felon%';
update crime_taxonomy set criminal = 0 where stat > 700;

update crime_taxonomy set misdemeanor = 1 where description like '%misd%';

update crime_taxonomy set violent = 1 where description like '%violence%';
update crime_taxonomy set violent = 1 where description like '%by force%';
update crime_taxonomy set violent = 1 where description like '%assault%';
update crime_taxonomy set violent = 1 where description like '%abuse%';
update crime_taxonomy set violent = 1 where description like '%hit and run%';
update crime_taxonomy set violent = 1 where description like '%hate%';
update crime_taxonomy set violent = 1 where description like '%arson%';
update crime_taxonomy set violent = 1 where description like '%kidnapping%';
update crime_taxonomy set violent = 1 where description like '%vandalism%';
update crime_taxonomy set violent = 1 where stat in (340,341,342,343,344,345,350,360);

update crime_taxonomy set gun = 1 where description like '%gun%';
update crime_taxonomy set gun = 1 where description like '%firearm%';
update crime_taxonomy set gun = 1 where description like '%shooting%';

update crime_taxonomy set weapon = 1 where description like '%gun%';
update crime_taxonomy set weapon = 1 where description like '%firearm%';
update crime_taxonomy set weapon = 1 where description like '%shooting%';
update crime_taxonomy set weapon = 1 where description like '%knife%';
update crime_taxonomy set weapon = 1 where description like '%weapon%';

delete from crime where year(date) < 2005 or year(date) = year(curdate());
delete from crime where stat in (select stat from crime_taxonomy where criminal = 0);
delete from crime where category in ('warrants','liquor','weapon','family','fraud','forgery','vagrancy','gambling','federal');
delete from crime where stat in (201,202,322,323,324,325,328,329,330,331,333,334,336,339,431,432);
delete from crime where city = '';
update crime c set city = (select primary_city from usps_zcta5 z where z.zip = c.zip) where c.zip like '9%';
update crime c set zip = '' where city is null;
/*delete from crime where zip not like '9%';*/
