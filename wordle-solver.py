# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Guess 1
# MAGIC 
# MAGIC Guess **raved** and insert the Wordle response into the box above:
# MAGIC 
# MAGIC * 0 = black/gray (no match)
# MAGIC * 1 = yellow box (right letter, wrong spot)
# MAGIC * 2 = green box (right letter, right spot)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Guess 2
# MAGIC 
# MAGIC Once you're ready, run the next command ... the result is your next guess ....

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wordleGuesses;
# MAGIC DROP TABLE IF EXISTS possibleWordles;
# MAGIC DROP TABLE IF EXISTS nextGuess;
# MAGIC CREATE TABLE wordleGuesses as
# MAGIC with guess1 as (
# MAGIC select 'raved' as guess),
# MAGIC 
# MAGIC result as (
# MAGIC select 1 as guessnum, (select guess from guess1) as guess, getArgument('WordleBox1') as pos1, getArgument('WordleBox2') as pos2, getArgument('WordleBox3') as pos3, getArgument('WordleBox4') as pos4, getArgument('WordleBox5') as pos5 )
# MAGIC 
# MAGIC select * from result;
# MAGIC 
# MAGIC CREATE TABLE possibleWordles AS
# MAGIC 
# MAGIC with
# MAGIC possibilities as (
# MAGIC select wordle as possible from wordle inner join wordleGuesses r  on r.guessNum = 1 and r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=r.guess )
# MAGIC 
# MAGIC select * from possibilities;
# MAGIC 
# MAGIC CREATE TABLE nextGuess AS
# MAGIC with
# MAGIC dist as (
# MAGIC select power(abs((count(1) - 1400)),2) as variance, wordle from wordle inner join possibleWordles p on p.possible=wordle.wordle where match not in (4, 5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select  sum(variance) as score, wordle as guess from dist  group by wordle )
# MAGIC 
# MAGIC select guess from guess2 order by score limit 1;
# MAGIC 
# MAGIC select * from nextGuess;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Guess 3
# MAGIC 
# MAGIC Same as before, enter the guess in Wordle and then update the widgets with the proper combination of 0s, 1s, and 2s, and then run the next command ...

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS possibleWordles2;
# MAGIC 
# MAGIC 
# MAGIC with guess as (
# MAGIC select guess from nextGuess),
# MAGIC 
# MAGIC result as (
# MAGIC select 2 as guessnum, (select guess from guess) as guess, getArgument('WordleBox1') as pos1, getArgument('WordleBox2') as pos2, getArgument('WordleBox3') as pos3, getArgument('WordleBox4') as pos4, getArgument('WordleBox5') as pos5 )
# MAGIC 
# MAGIC insert into wordleGuesses
# MAGIC select * from result;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE possibleWordles2 AS
# MAGIC with
# MAGIC possibilities as (
# MAGIC select wordle as possible from wordle inner join possibleWordles p on p.possible = wordle.wordle inner join wordleGuesses r on r.guessnum =2 and r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from nextguess) )
# MAGIC 
# MAGIC 
# MAGIC select * from possibilities;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nextGuess;
# MAGIC CREATE TABLE nextGuess AS
# MAGIC with
# MAGIC dist as (
# MAGIC select power(abs((count(1) - 1400)),2) as variance, wordle from wordle inner join possibleWordles2 p on p.possible=wordle.wordle where match not in (4, 5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select  sum(variance) as score, wordle as guess from dist  group by wordle )
# MAGIC 
# MAGIC select guess from guess2 order by score limit 1;
# MAGIC 
# MAGIC select * from nextGuess;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Guess 4
# MAGIC 
# MAGIC Still no luck? This should do the trick ...

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS possibleWordles3;
# MAGIC 
# MAGIC 
# MAGIC with guess as (
# MAGIC select guess from nextGuess),
# MAGIC 
# MAGIC result as (
# MAGIC select 3 as guessnum, (select guess from guess) as guess, getArgument('WordleBox1') as pos1, getArgument('WordleBox2') as pos2, getArgument('WordleBox3') as pos3, getArgument('WordleBox4') as pos4, getArgument('WordleBox5') as pos5 )
# MAGIC 
# MAGIC insert into wordleGuesses
# MAGIC select * from result;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE possibleWordles3 AS
# MAGIC with
# MAGIC possibilities as (
# MAGIC select wordle as possible from wordle inner join possibleWordles2 p on p.possible = wordle.wordle inner join wordleGuesses r on r.guessnum =3 and r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from nextguess) )
# MAGIC 
# MAGIC 
# MAGIC select * from possibilities;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nextGuess;
# MAGIC CREATE TABLE nextGuess AS
# MAGIC with
# MAGIC dist as (
# MAGIC select power(abs((count(1) - 1400)),2) as variance, wordle from wordle inner join possibleWordles3 p on p.possible=wordle.wordle where match not in (5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select  sum(variance) as score, wordle as guess from dist  group by wordle )
# MAGIC 
# MAGIC select guess from guess2 order by score limit 1;
# MAGIC 
# MAGIC select * from nextGuess;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Guess 5
# MAGIC 
# MAGIC If you made it here, congratulations! Your Wordle word is extremely rare ...

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS possibleWordles4;
# MAGIC 
# MAGIC 
# MAGIC with guess as (
# MAGIC select guess from nextGuess),
# MAGIC 
# MAGIC result as (
# MAGIC select 4 as guessnum, (select guess from guess) as guess, getArgument('WordleBox1') as pos1, getArgument('WordleBox2') as pos2, getArgument('WordleBox3') as pos3, getArgument('WordleBox4') as pos4, getArgument('WordleBox5') as pos5 )
# MAGIC 
# MAGIC insert into wordleGuesses
# MAGIC select * from result;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE possibleWordles4 AS
# MAGIC with
# MAGIC possibilities as (
# MAGIC select wordle as possible from wordle inner join possibleWordles3 p on p.possible = wordle.wordle inner join wordleGuesses r on r.guessnum = 4 and r.pos1 =wordle.pos1 and r.pos2=wordle.pos2 and r.pos3=wordle.pos3 and r.pos4=wordle.pos4 and r.pos5=wordle.pos5 and wordle.guess=(select guess from nextguess) )
# MAGIC 
# MAGIC 
# MAGIC select * from possibilities;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS nextGuess;
# MAGIC CREATE TABLE nextGuess AS
# MAGIC with
# MAGIC dist as (
# MAGIC select power(abs((count(1) - 1400)),2) as variance, wordle from wordle inner join possibleWordles4 p on p.possible=wordle.wordle where match not in (5) group by match, wordle ),
# MAGIC 
# MAGIC guess2 as (
# MAGIC select  sum(variance) as score, wordle as guess from dist  group by wordle )
# MAGIC 
# MAGIC select guess from guess2 order by score limit 1;
# MAGIC 
# MAGIC select * from nextGuess;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from possibleWordles2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from wordleGuesses order by guessNum

# COMMAND ----------


