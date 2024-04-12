#!/usr/bin/env python
# coding: utf-8

# In[1]:


##score 1
ami=['I21','I22','I252'] #Acute myocardial infarction
chf=['I50'] #Congestive heart failure
pvd=['I71', 'I790', 'I739', 'R02', 'Z958', 'Z959'] #Peripheral vascular disease
cva=['I60', 'I61', 'I62', 'I63', 'I65', 'I66', 'G450', 'G451', 'G452', 'G458', 'G459', 'G46', 'I64', 'G454', 'I670', 'I671', 'I672', 'I674', 'I675', 'I676', 'I677', 'I678', 'I679', 'I681', 'I682', 'I688', 'I69'] #Cerebral vascular accident
dem=['F00', 'F01', 'F02', 'F051'] #Dementia
pul=['J40', 'J41', 'J42', 'J44', 'J43', 'J45', 'J46', 'J47', 'J67', 'J44', 'J60', 'J61', 'J62', 'J63', 'J66', 'J64', 'J65'] #Pulmonary disease
ctd=['M32', 'M34', 'M332', 'M053', 'M058', 'M059', 'M060', 'M063', 'M069', 'M050', 'M052', 'M051', 'M353'] #Connective tissue disorder
pep=['K25', 'K26', 'K27', 'K28'] #Peptic ulcer
liv=['K702', 'K703', 'K73', 'K717', 'K740', 'K742', 'K746', 'K743', 'K744', 'K745'] #Liver disease
dia=['E109', 'E119', 'E139', 'E149', 'E101', 'E111', 'E131', 'E141', 'E105', 'E115', 'E135', 'E145'] #Diabetes

##score 2
dic=['E102', 'E112', 'E132', 'E142', 'E103', 'E113', 'E133', 'E143', 'E104', 'E114', 'E134', 'E144'] #Diabetes complications
par=['G81', 'G041', 'G820', 'G821', 'G822'] #Paraplegia
red=['N03', 'N052', 'N053', 'N054', 'N055', 'N056', 'N072', 'N073', 'N074', 'N01', 'N18', 'N19', 'N25'] #Renal disease
can=['C0', 'C1', 'C2', 'C3', 'C40', 'C41', 'C43', 'C45', 'C46', 'C47', 'C48', 'C49', 'C5', 'C6', 'C70', 'C71', 'C72', 'C73', 'C74', 'C75', 'C76', 'C80', 'C81', 'C82', 'C83', 'C84', 'C85', 'C883', 'C887', 'C889', 'C900', 'C901', 'C91', 'C92', 'C93', 'C940', 'C941', 'C942', 'C943', 'C9451', 'C947', 'C95', 'C96'] #cancer

##score 3
mec=['C77', 'C78', 'C79', 'C80'] #Metastatic cancer
sld=['K729', 'K766', 'K767', 'K721'] #Severe liver disease

##score 6
hiv=['B20', 'B21', 'B22', 'B23', 'B24'] #HIV


# In[6]:


## import
import pandas as pd
from pandas import DataFrame

def score_group(t, d):
    s=0
    for i in range(len(d)): 
        if len(t[t.iloc[:,1].str.contains(d[i])])>0 : s=s+1
    return s

def score_sum(id, table):
    table_id=table[table.iloc[:,0]==id]
    score_id=[]
    score_1=[ami, chf, pvd, cva, dem, pul, ctd, pep, liv, dia]
    score_2=[dic, par, red, can]
    score_3=[mec, sld]
    score_6=[hiv]
    for i in range(len(score_1)):
        if score_group(table_id, score_1[i])>0 :score_id.append(1)
        else:score_id.append(0)
            
    for i in range(len(score_2)):
        if score_group(table_id, score_2[i])>0 :score_id.append(2)
        else:score_id.append(0)
            
    for i in range(len(score_3)):
        if score_group(table_id, score_3[i])>0 :score_id.append(3)
        else:score_id.append(0)
            
    for i in range(len(score_6)):
        if score_group(table_id, score_6[i])>0 :score_id.append(6)
        else:score_id.append(0)
    score_id.append(sum(score_id))
    
    return score_id

def score_table(table):
    pid=table.iloc[:,0].drop_duplicates()
    pid=pid.reset_index()
    pid[['Acute myocardial infarction', 'Congestive heart failure', 'Peripheral vascular disease', 'Cerebral vascular accident', 'Dementia', 'Pulmonary disease', 'Connective tissue disorder', 'Peptic ulcer', 'Liver disease', 'Diabetes', 'Diabetes complications', 'Paraplegia', 'Renal disease', 'Cancer', 'Metastatic cancer', 'Severe liver disease', 'HIV', 'CCI']]=pd.DataFrame([[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]],index=pid.index)
    for i in range(len(pid)):
        pid.iloc[i,2:]=score_sum(pid.iloc[i,1], table)
        if i%100==0:print(i,'/',len(pid))
    return pid


# In[ ]:


##사람기준_CDM
file='t02.csv'
data=pd.read_csv(file, sep=',', encoding='CP949')
data_1=pd.concat([data.loc[:,'person_id'], data['condition_source_value'].fillna('')], axis=1)
extra=data.loc[:,['person_id', 'group_hypo']].drop_duplicates()

print(data_1)

result=score_table(data_1)
result=pd.merge(extra, result, how='left', on='person_id')
result=result.drop_duplicates()
result.drop('index', axis=1).to_csv(str(file.split('.')[0]+'_cci.csv'), index=False)

