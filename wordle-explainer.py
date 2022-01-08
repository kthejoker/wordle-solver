# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Which word divides the word set up most evenly?
# MAGIC 
# MAGIC If you've ever played "higher or lower" when guessing the number, you probably know the best strategy is to pick the number in the middle of the guessing range ... this maximizes the *information gain* from the answer - whether it's "higher" or "lower", you eliminate 50% of the possible answers. All other guesses are suboptimal!
# MAGIC 
# MAGIC For Wordle, we have the same basic strategy: ignoring 4-matches (which only occur in **1.82%** of all possible guesses), the "ideal" guess would divide up the remaining possible words into 4 equal groups: words with 0 matches with the guess, 1 match with the guess, 2 matches with the guess, and 3 matches with the guess.
# MAGIC 
# MAGIC That way, no matter what the result of the guess is, you've maximally eliminated words from the wordset.
# MAGIC 
# MAGIC Since no word actually does this, we can calculate the word that divides the 4 groups up the most evenly - that is, has the lowest variance (the sum of the squares of the difference between the *ideal* split and the actual splits the word produces.)
# MAGIC 
# MAGIC As an example, let's look at the word **house**:

# COMMAND ----------

# MAGIC %sql
# MAGIC with rawResults as (
# MAGIC select
# MAGIC match, count(1) as actualSplit, (select count(1) from wordle where wordle='house' and match not in (4,5)) as totalCount
# MAGIC from wordle
# MAGIC where wordle='house'
# MAGIC and match not in (4,5) -- ignoring unlikely cases
# MAGIC group by match
# MAGIC )
# MAGIC ,
# MAGIC calcs as (
# MAGIC select match, actualSplit, totalCount / 4 as idealSplit, abs(actualSplit - (totalCount / 4 )) as difference, power ( abs(actualSplit - (totalCount / 4 )) , 2) as variance
# MAGIC from rawresults)
# MAGIC 
# MAGIC select * , (select sum(variance) as totalVariance from calcs) as totalVariance
# MAGIC from calcs
# MAGIC order by match

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We see that the *ideal split* for house is each of the 4 buckets should have ~1401 potential words. We then calculate the difference between that ideal split and the *actual splits*, square it (to punish particularly bad splits), and sum up those values for a total variance of **1930070.75**.
# MAGIC 
# MAGIC If we do this same math for every potential 5 letter word in our dictionary, and then find the word with the lowest total variance, we get ...

# COMMAND ----------

# MAGIC %sql
# MAGIC with rawResults as (
# MAGIC select
# MAGIC wordle, match, count(1) as actualSplit, 5600 as totalCount
# MAGIC from wordle
# MAGIC where match not in (4,5) -- ignoring unlikely cases
# MAGIC group by wordle, match
# MAGIC )
# MAGIC ,
# MAGIC calcs as (
# MAGIC select wordle, match, actualSplit, totalCount / 4 as idealSplit, abs(actualSplit - (totalCount / 4 )) as difference, power ( abs(actualSplit - (totalCount / 4 )) , 2) as variance
# MAGIC from rawresults)
# MAGIC 
# MAGIC select wordle, sum(variance) as totalVariance
# MAGIC from calcs
# MAGIC group by wordle
# MAGIC order by totalVariance limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Version 1: You are the Wordlemaster
# MAGIC 
# MAGIC To demonstrate how this works a little more, you get to pick the Wordle that the computer will try to guess. Just enter it in the text widget at the top of this notebook.
# MAGIC 
# MAGIC Ready?
# MAGIC 
# MAGIC So we have a repeatable 4 step process ...
# MAGIC 
# MAGIC 1) Make a guess and get back the "Wordle boxes" against the actual Wordle.
# MAGIC 2) Reduce our dictionary to the words that match the Wordle box criteria.
# MAGIC 3) Find the word that produces the most "even" split within the remaining words.
# MAGIC 4) Repeat until we've guessed the word.
# MAGIC 
# MAGIC # Step 1: Guess and Result
# MAGIC 
# MAGIC Since **raved** is the closest thing we have to the "middle number" in Wordle, we'll guess that first. Then we'll compare it against the word you entered above (don't worry, the computer doesn't need to see the word, it's just a convenient way to generate the "Wordle feedback" of green and yellow boxes.)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wordleGuesses;
# MAGIC CREATE TABLE wordleGuesses
# MAGIC AS
# MAGIC with guess1 as (
# MAGIC select 'raved' as guess),
# MAGIC 
# MAGIC result as (
# MAGIC select 1 as guessnum, guess, pos1, pos2, pos3, pos4, pos5 from wordle where guess= (select guess from guess1) and wordle=lower(getArgument('secretWordle'))
# MAGIC )
# MAGIC 
# MAGIC  SELECT *
# MAGIC from result;
# MAGIC 
# MAGIC 
# MAGIC select * from wordleGuesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 2: Reduce the Dictionary
# MAGIC 
# MAGIC For our guess, we get back 5 "positions" with either a 0 (not in the word), 1 (in the word but in the wrong spot), or 2 (in the right spot). With this information, we reduce our dictionary to the remaining possible words:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS possibleWordles;
# MAGIC CREATE TABLE possibleWordles as 
# MAGIC with possibilities as (
# MAGIC select wordle as possible from wordle inner join wordleGuesses r  on r.guessnum=1 and r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from wordleGuesses where guessnum=1) )
# MAGIC select * from possibilities;
# MAGIC 
# MAGIC select * from possibleWordles;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 3: Find the next "best" guess
# MAGIC 
# MAGIC And now we just perform the same splitting calculation from earlier, but only within the remaining possible words ....

# COMMAND ----------

# MAGIC %sql
# MAGIC with
# MAGIC dist as (
# MAGIC select power(abs((count(1) - 1400)),2) as variance, wordle from wordle inner join possibleWordles p on p.possible=wordle.wordle where match not in (4, 5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select wordle as guess, sum(variance) as totalVariance from dist  group by wordle )
# MAGIC 
# MAGIC select * from guess2 order by totalVariance limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 4: Repeat Until We Guess the Wordle
# MAGIC 
# MAGIC ... and so on. For convenience I've combined all the code above into a single notebook command. It's an extremely cocky computer, so it tries to guess your word in just 4 tries instead of Wordle's generous 6. See how it does!

# COMMAND ----------

# MAGIC %sql
# MAGIC with guess1 as (
# MAGIC select 'raved' as guess),
# MAGIC 
# MAGIC result as (
# MAGIC select 1 as guessnum, guess, pos1, pos2, pos3, pos4, pos5 from wordle where guess= (select guess from guess1) and wordle=lower(getArgument('secretWordle'))
# MAGIC ), 
# MAGIC 
# MAGIC 
# MAGIC possibilities as (
# MAGIC select wordle as possible from wordle inner join result r  on r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from guess1) )
# MAGIC 
# MAGIC ,
# MAGIC dist as (
# MAGIC select power(abs(0.25 - (count(1) / 5600)),2) as variance, wordle from wordle inner join possibilities on possibilities.possible=wordle.wordle where match not in (4, 5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select  sum(variance) as score, wordle as guess from dist  group by wordle ),
# MAGIC 
# MAGIC result2 as (
# MAGIC select 2 as guessnum, guess, pos1, pos2, pos3, pos4, pos5 from wordle where guess= (select guess from guess2 order by score limit 1) and wordle=lower(getArgument('secretWordle'))
# MAGIC ), 
# MAGIC 
# MAGIC possibilities2 as (
# MAGIC select wordle as possible from wordle inner join possibilities p on p.possible = wordle.wordle inner join result2 r on r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from guess2 order by score limit 1) )
# MAGIC 
# MAGIC ,
# MAGIC 
# MAGIC 
# MAGIC dist2 as (
# MAGIC select power(abs(0.2 - (count(1) / 5600)),2) as variance, wordle from wordle inner join possibilities2 on possibilities2.possible=wordle.wordle where match not in (5) group by match, wordle ),
# MAGIC 
# MAGIC guess3 as (
# MAGIC select sum(variance) as score, wordle as guess from dist2  group by wordle),
# MAGIC 
# MAGIC result3 as (
# MAGIC select 3 as guessnum, guess, pos1, pos2, pos3, pos4, pos5 from wordle where guess= (select guess from guess3 order by score limit 1) and wordle=lower(getArgument('secretWordle'))
# MAGIC ), 
# MAGIC 
# MAGIC possibilities3 as (
# MAGIC select wordle as possible from wordle inner join possibilities2 p on p.possible = wordle.wordle inner join result3 r on r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from guess3 order by score limit 1) )
# MAGIC 
# MAGIC ,
# MAGIC 
# MAGIC 
# MAGIC dist3 as (
# MAGIC select power(abs(0.2 - (count(1) / 5600)),2) as variance, wordle from wordle inner join possibilities3 p on p.possible=wordle.wordle where match not in (5) group by match, wordle ),
# MAGIC 
# MAGIC guess4 as (
# MAGIC select sum(variance) as score, wordle as guess from dist3  group by wordle ),
# MAGIC 
# MAGIC result4 as (
# MAGIC select 4 as guessnum, guess, pos1, pos2, pos3, pos4, pos5 from wordle where guess= (select guess from guess4 order by score limit 1) and wordle=lower(getArgument('secretWordle'))
# MAGIC ), 
# MAGIC 
# MAGIC allresults as (
# MAGIC select * from result union
# MAGIC select * from result2 union
# MAGIC select * from result3 union
# MAGIC select * from result4)
# MAGIC 
# MAGIC select * from allresults order by guessnum

# COMMAND ----------


