-- Load tweets and get id and text
tweets = LOAD 'tweets/*' USING PigStorage('|') AS (date:chararray, id:chararray, handle, name, location, index1, index2, index3, index4, text:chararray);
tweets_filtered = FOREACH tweets GENERATE CONCAT(id, date) AS ID, text;

-- Tokenize and lowercase them
words_tokenized = FOREACH tweets_filtered GENERATE ID, FLATTEN(TOKENIZE(text)) AS twitter_words;
words_lower = FOREACH words_tokenized GENERATE ID, LOWER(twitter_words) AS twitter_words_lower;

-- Load the good words
good = LOAD 'good_words/*' USING PigStorage('\n') AS (good_word:chararray);
good_score = FOREACH good GENERATE good_word, 1 AS g_score;

-- Join tweets and good words
good_tweets = JOIN words_lower BY twitter_words_lower, good_score BY good_word USING 'replicated';

-- Load the bad words
bad = LOAD 'bad_words/*'  USING PigStorage('\n') AS (bad_word:chararray);
bad_score = FOREACH bad GENERATE bad_word, -1 AS b_score;

-- Join tweets and bad words
bad_tweets = JOIN words_lower BY twitter_words_lower, bad_score BY bad_word USING 'replicated';

-- Take union of the two and group by id
good_bad = UNION good_tweets, bad_tweets;
grouped = GROUP good_bad BY $0;

-- Get the sentiment score
tweets_grouped = FOREACH grouped GENERATE $0 AS id, $1 AS tweet_tuple;
sentiment = FOREACH tweets_grouped GENERATE id, SUM(tweet_tuple.$3) AS tweet_sentiment;

-- Filter, count, and store the good tweets
good_filter = FILTER sentiment BY tweet_sentiment > 0;
positive = GROUP good_filter ALL;
positive_count = FOREACH positive GENERATE COUNT(good_filter);
STORE positive_count INTO 'tweets/positive' USING PigStorage();

-- Filter, count, and store the bad tweets
bad_filter = FILTER sentiment BY tweet_sentiment < 0;
negative = GROUP bad_filter ALL;
negative_count = FOREACH negative GENERATE COUNT(bad_filter);
STORE negative_count INTO 'tweets/negative' USING PigStorage();