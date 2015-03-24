records = LOAD 'wordCount.txt' USING PigStorage AS (line:chararray);

decLines = FILTER records BY (LOWER(line) matches '.*dec.*');
hackLines = FILTER records BY (LOWER(line) matches '.*hackathon.*');
chicLines = FILTER records BY (LOWER(line) matches '.*chicago.*');
javaLines = FILTER records BY (LOWER(line) matches '.*java.*');

decGroup = GROUP decLines ALL;
hackGroup = GROUP hackLines ALL;
chicGroup = GROUP chicLines ALL;
javaGroup = GROUP javaLines ALL;

decCount = FOREACH decGroup GENERATE COUNT(decLines);
hackCount = FOREACH hackGroup GENERATE COUNT(hackLines);
chicCount = FOREACH chicGroup GENERATE COUNT(chicLines);
javaCount = FOREACH javaGroup GENERATE COUNT(javaLines);

decGen = FOREACH decCount GENERATE 'Dec', $0;
hackGen = FOREACH hackCount GENERATE 'Hackathon', $0;
chicGen = FOREACH hackCount GENERATE 'Chicago', $0;
javaGen = FOREACH javaCount GENERATE 'Java', $0;

combined_records = UNION decGen, hackGen, chicGen, javaGen;

dec0 = FOREACH records GENERATE 'Dec', 0;
dec0 = DISTINCT dec0;      
hack0 = FOREACH records GENERATE 'Hackathon', 0;
hack0 = DISTINCT hack0;
chic0 = FOREACH records GENERATE 'Chicago', 0; 
chic0 = DISTINCT chic0;
java0 = FOREACH records GENERATE 'Java', 0; 
java0 = DISTINCT java0;

union_records = UNION dec0, hack0, chic0, java0;
final_union = UNION combined_records, union_records;

final_group = GROUP final_union by $0;
final_group = FOREACH final_group GENERATE group, MAX(final_union.$1);

result = FOREACH (GROUP final_group BY RANDOM()) GENERATE FLATTEN(final_group);

STORE result INTO 'wordCountOP';

