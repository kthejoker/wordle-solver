# Databricks notebook source
import pandas as pd
import numpy as np
import os
#ps.set_option('compute.ops_on_diff_frames', True)

data = pd.read_csv("file:/Workspace/Repos/kyle.hale@databricks.com/wordle-tree/sgb_words.txt", header = None)

def cartesian_product(*arrays):
    la = len(arrays)
    dtype = np.result_type(*arrays)
    arr = np.empty([len(a) for a in arrays] + [la], dtype=dtype)
    for i, a in enumerate(np.ix_(*arrays)):
        arr[...,i] = a
    return arr.reshape(-1, la) 

def cartesian_product_generalized(left, right):
    la, lb = len(left), len(right)
    idx = cartesian_product(np.ogrid[:la], np.ogrid[:lb])
    return pd.DataFrame(
        np.column_stack([left.values[idx[:,0]], right.values[idx[:,1]]]), columns=['wordle', 'guess'])

def wordle_check(row):
    count = 0
    for letter in set(row['wordle']):
       count += row['guess'].count(letter)    
    return count

wordle_set = cartesian_product_generalized(data, data)
wordle_set['match'] = wordle_set.apply(wordle_check, axis = 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wordle;

# COMMAND ----------

w_df = wordle_set.to_spark()
w_df.write.format('delta').saveAsTable('wordle')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE wordle ADD COLUMN pos1 int, pos2 int, pos3 int, pos4 int, pos5 int, letter1 string, letter2 string, letter3 string, letter4 string, letter5 string;
# MAGIC 
# MAGIC 
# MAGIC update wordle set 
# MAGIC pos1= CASE WHEN substr(wordle,1,1)=substr(guess,1,1) then 2 WHEN instr(concat (
# MAGIC case when substr(wordle,1,1)=substr(guess,1,1) then '' else substr(wordle,1,1) end ,
# MAGIC case when substr(wordle,2,1)=substr(guess,2,1) then '' else substr(wordle,2,1) end ,
# MAGIC case when substr(wordle,3,1)=substr(guess,3,1) then '' else substr(wordle,3,1) end ,
# MAGIC case when substr(wordle,4,1)=substr(guess,4,1) then '' else substr(wordle,4,1) end ,
# MAGIC case when substr(wordle,5,1)=substr(guess,5,1) then '' else substr(wordle,5,1) end ),substr(guess,1,1)) > 0 then 1 else 0 end,
# MAGIC pos2= CASE WHEN substr(wordle,2,1)=substr(guess,2,1) then 2 WHEN instr(concat (
# MAGIC case when substr(wordle,1,1)=substr(guess,1,1) then '' else substr(wordle,1,1) end ,
# MAGIC case when substr(wordle,2,1)=substr(guess,2,1) then '' else substr(wordle,2,1) end ,
# MAGIC case when substr(wordle,3,1)=substr(guess,3,1) then '' else substr(wordle,3,1) end ,
# MAGIC case when substr(wordle,4,1)=substr(guess,4,1) then '' else substr(wordle,4,1) end ,
# MAGIC case when substr(wordle,5,1)=substr(guess,5,1) then '' else substr(wordle,5,1) end ),substr(guess,2,1)) > 0 then 1 else 0 end,
# MAGIC pos3= CASE WHEN substr(wordle,3,1)=substr(guess,3,1) then 2 WHEN instr(concat (
# MAGIC case when substr(wordle,1,1)=substr(guess,1,1) then '' else substr(wordle,1,1) end ,
# MAGIC case when substr(wordle,2,1)=substr(guess,2,1) then '' else substr(wordle,2,1) end ,
# MAGIC case when substr(wordle,3,1)=substr(guess,3,1) then '' else substr(wordle,3,1) end ,
# MAGIC case when substr(wordle,4,1)=substr(guess,4,1) then '' else substr(wordle,4,1) end ,
# MAGIC case when substr(wordle,5,1)=substr(guess,5,1) then '' else substr(wordle,5,1) end ),substr(guess,3,1)) > 0 then 1 else 0 end,
# MAGIC pos4= CASE WHEN substr(wordle,4,1)=substr(guess,4,1) then 2 WHEN instr(concat (
# MAGIC case when substr(wordle,1,1)=substr(guess,1,1) then '' else substr(wordle,1,1) end ,
# MAGIC case when substr(wordle,2,1)=substr(guess,2,1) then '' else substr(wordle,2,1) end ,
# MAGIC case when substr(wordle,3,1)=substr(guess,3,1) then '' else substr(wordle,3,1) end ,
# MAGIC case when substr(wordle,4,1)=substr(guess,4,1) then '' else substr(wordle,4,1) end ,
# MAGIC case when substr(wordle,5,1)=substr(guess,5,1) then '' else substr(wordle,5,1) end ),substr(guess,4,1)) > 0 then 1 else 0 end,
# MAGIC pos5= CASE WHEN substr(wordle,5,1)=substr(guess,5,1) then 2 WHEN instr(concat (
# MAGIC case when substr(wordle,1,1)=substr(guess,1,1) then '' else substr(wordle,1,1) end ,
# MAGIC case when substr(wordle,2,1)=substr(guess,2,1) then '' else substr(wordle,2,1) end ,
# MAGIC case when substr(wordle,3,1)=substr(guess,3,1) then '' else substr(wordle,3,1) end ,
# MAGIC case when substr(wordle,4,1)=substr(guess,4,1) then '' else substr(wordle,4,1) end ,
# MAGIC case when substr(wordle,5,1)=substr(guess,5,1) then '' else substr(wordle,5,1) end ),substr(guess,5,1)) > 0 then 1 else 0 end,
# MAGIC letter1 = substr(wordle,1,1),
# MAGIC letter2 = substr(wordle,2,1),
# MAGIC letter3 = substr(wordle,3,1),
# MAGIC letter4 = substr(wordle,4,1),
# MAGIC letter5 = substr(wordle,5,1)
