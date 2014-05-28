--
-- Load files
A = LOAD 'jobs_sample/*' USING PigStorage('\t') AS (id:chararray, description:chararray);

-- Tokenize words
B = FOREACH A GENERATE id, FLATTEN(TOKENIZE(description)) AS upper_token_desc;

-- Lowercase words
X = FOREACH B GENERATE id, LOWER(upper_token_desc) AS token_desc;

-- Load stopwords
C = LOAD 'jobs_files/stopwords-en.txt' USING PigStorage() AS (stopword:chararray);

-- Left outer join on stopwords
D = JOIN X BY token_desc LEFT OUTER, C BY stopword USING 'replicated';

-- filter out where the stopword is null, meaning it is not in the stopword list
E = FILTER D BY stopword IS NULL;

-- remove the stopword column that we don't need.
F = FOREACH E GENERATE id, token_desc;

-- Register the stemming function
REGISTER 'stem_udf/STEM.jar';

-- Apply stemming on non-stopword words
I = FOREACH F GENERATE id, STEM(token_desc) AS token_desc;

-- Load the dictionary
J = LOAD 'jobs_files/dictionary.txt' USING PigStorage() AS (dict_word:chararray);

-- Group the words
K = COGROUP I BY token_desc, J BY dict_word;

-- Find real words and not real words
Correct = FILTER K BY NOT IsEmpty($2.dict_word);

Incorrect = FILTER K BY IsEmpty($2.dict_word);

-- Cross the incorrect words and dictionary
L = CROSS Incorrect, J;

-- Register Levenshtein function
REGISTER 'lev_udf/LEV.jar';

-- Clean object
M = FOREACH L GENERATE FLATTEN($1.$0) AS id, FLATTEN($1.$1) AS job_word, $3 AS dict_word;

-- Get Levenshtein distance
Temp = FOREACH M GENERATE id, job_word, dict_word, LEV(job_word, dict_word) AS lev_dist;

-- Group
N = GROUP Temp BY id..job_word;

-- Get minimum Levenshtein distance and that word
O = FOREACH N {
  ordered_groups = ORDER Temp BY lev_dist ASC;
  top = LIMIT ordered_groups 1;
  GENERATE FLATTEN(group), FLATTEN(top.dict_word), FLATTEN(top.lev_dist);
};

-- Clean object
JobCorrect = FOREACH Correct GENERATE FLATTEN($1.$0) AS id, FLATTEN($2) AS job_word;

JobFixed = FOREACH O GENERATE $0 AS id, $2 AS job_word;

-- Take union of correct words and Levenshtein words
JobUnion = UNION JobCorrect, JobFixed;

-- Group and clean objects
P = GROUP JobUnion BY id;

Q = FOREACH P GENERATE $0 AS id, $1.$1 AS word;

-- Store results
STORE Q INTO 'jobs_results' USING PigStorage();
--