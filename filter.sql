
delete from crime_dictionary where term in ('ii','iii','fel','etc','veh','via','non','agg','adw','mkt','oz','iv','less','grand');
delete from crime_dictionary where word in ('yrs','years');
delete from crime_dictionary where word = 'vehicle' and category ='grand theft auto';
delete from crime_dictionary where word = 'felony' and category ='felonies';
delete from crime_dictionary where word like 'auto%';

delete from crime_wordnet where term1 in ('ii','iii','fel','etc','veh','via','non','agg','adw','mkt','oz','iv','less','grand');
delete from crime_wordnet where term2 in ('ii','iii','fel','etc','veh','via','non','agg','adw','mkt','oz','iv','less','grand');
delete from crime_wordnet where word1 in ('yrs','years');
delete from crime_wordnet where word2 in ('yrs','years');
delete from crime_wordnet where word1 = 'vehicle' and category ='grand theft auto';
delete from crime_wordnet where word2 = 'vehicle' and category ='grand theft auto';
delete from crime_wordnet where word1 = 'felony' and category ='felonies';
delete from crime_wordnet where word2 = 'felony' and category ='felonies';
delete from crime_wordnet where word1 like 'auto%' or word2 like 'auto%';
