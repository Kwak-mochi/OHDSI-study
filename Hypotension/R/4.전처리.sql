####이 코드는 R을 통해 데이터를 전처리(outlier 처리, imputation, psm)을 진행합니다.
#### step info
#### step 1. read and merge
#### step 2. remove outlier and imputati
#### step 3. Propensity Score matching

#install.package("moonBook")
#install.package("readr")
#install.package("dplyr")
#install.package("naniar")
#install.package("miceRanger")
#install.package("MatchIt")
#install.package("ggplot2")
#install.package("cobalt")


#### step 1. read and merge
library(moonBook)
library(readr)
library(dplyr)

raw <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/raw.csv', col_names = T,na=c("", "NULL"))

t01 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t01.csv', col_names = T,na=c("", "NULL"))
t02 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t02_cci.csv', col_names = T,na=c("", "NULL"))
t03 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t03.csv', col_names = T,na=c("", "NULL"))
t04 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t04.csv', col_names = T)
t05 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t05.csv', col_names = T)
t06 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t06.csv', col_names = T,na=c("", "NULL"))
t07 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t07.csv', col_names = T,na=c("", "NULL"))
t08 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/t08.csv', col_names = T,na=c("", "NULL"))


##table merge : data_raw
raw[raw$drug_source_value == "AAP5I", "drug_source_value_group"]="AAP"
raw[raw$drug_source_value == "AAP1I", "drug_source_value_group"]="AAP"
raw[raw$drug_source_value == "PAAPI", "drug_source_value_group"]="PAA"
data_raw <- subset(raw, select = c(person_id,drug_source_value, drug_source_value_group))

#t01
data_raw<-inner_join(data_raw, subset(t01, select= c(person_id,group_hypo,age,sex,height,weight,index_year)), by='person_id')
data_raw[,"BMI"] <- data_raw$weight*10000/(data_raw$height*data_raw$height)
data_raw[data_raw$index_year >= 2012 & data_raw$index_year <= 2015, "index_year_group"]="2012-2015"
data_raw[data_raw$index_year >= 2016 & data_raw$index_year <= 2018, "index_year_group"]="2016-2018"
data_raw[data_raw$index_year >= 2019 & data_raw$index_year <= 2021,"index_year_group"]="2019-2021"

#t02
data_raw<-inner_join(data_raw, subset(t02, select= c(person_id,CCI)), by='person_id')
data_raw[data_raw$CCI == 0, "cci_group"]="cci_0"
data_raw[data_raw$CCI > 0 & data_raw$CCI < 3, "cci_group"]="cci_1-2"
data_raw[data_raw$CCI > 2 ,"cci_group"]="cci_3"

#t03
data_raw<-inner_join(data_raw, subset(t03, select= c(person_id, surgery_bfiv, icu, drug_bfiv)), by='person_id')

#t04
data_raw<-inner_join(data_raw, subset(t04, select= c(person_id, infection, organ_dysfunction, new_sepsis, dig_sepsis, sepsis, antibio)), by='person_id')

#t05
data_raw<-inner_join(data_raw, subset(t05, select= c(person_id, hypertension, diabetes_mellitus,heart_failure,chronic_kidney_disease,liver_disease,cerebrovascular,metastatic_cancer,hematologic_malignancy,solid_tumor)), by='person_id')
#t06
data_raw<-inner_join(data_raw, subset(t06, select= c(person_id,mbp,pr,rr,bt,spo2)), by='person_id')
#t07
data_raw<-inner_join(data_raw, subset(t07, select= c(person_id,ph_48,pco2_48,po2_48,hco3_48,crp_48,lactate_48,wbc_48,hemoglobin_48,platelet_48,creatinine_48,bilirubin_48,bun_48)), by='person_id')
#t08
data_raw<-inner_join(data_raw, subset(t08, select= c(person_id,dead_hosp,visit_to_drug, drug_to_end, visit_to_end)), by='person_id')

data_raw[data_raw$person_id>0, "re_person_id"]=paste0("id_",data_raw$person_id)
data_raw <- subset(data_raw, select= -c(person_id))

#data check
#mytable(data_raw)
#summary(data_raw)
#mytable(group_hypo~.-re_person_id, data=data_raw)



#### step 2. remove outlier and imputation

##remove outlier : data1
#function1(기준치 이하값 기준치로 변경)
ch_min <- function(dataset, col, min) {
  dataset[, col, drop=T] <- ifelse(dataset[, col, drop=T] < min, min, dataset[, col, drop=T])
  return (dataset)
}

#function2(기준치 이하,이상값 기준치로 변경)
ch_minmax <- function(dataset, col, min, max) {
  dataset[, col, drop=T] <- ifelse(dataset[, col, drop=T] < min, min, dataset[, col, drop=T])
  dataset[, col, drop=T] <- ifelse(dataset[, col, drop=T] > max, max, dataset[, col, drop=T])
  return (dataset)
}

data1<-data_raw
data1<-ch_min(data1, "height", 100)
data1<-ch_min(data1, "weight", 25)
data1[,"BMI"] <- data1$weight*10000/(data1$height*data1$height)
data1<-ch_minmax(data1, "mbp", 24.3, 174.7)
data1<-ch_minmax(data1, "pr", 31, 212)
data1<-ch_minmax(data1, "rr", 8, 52)
data1<-ch_minmax(data1, "bt", 31.0, 42.0)
data1<-ch_minmax(data1, "spo2", 50, 100)

#data check
#mytable(data1)
#mytable(group_hypo~.-re_person_id, data=data1)
#summary(data1)

#결측치 시각화
library(naniar)
naniar::gg_miss_var(data1, show_pct=TRUE)
naniar::gg_miss_upset(data1)

##imputation : data2
library(miceRanger)
data1_cut <- subset(data1, select= c(re_person_id, group_hypo, age, sex, BMI, CCI, index_year, surgery_bfiv, sepsis, icu, drug_bfiv, 
                                     hypertension, diabetes_mellitus, heart_failure, chronic_kidney_disease, liver_disease, cerebrovascular, 
                                     metastatic_cancer, hematologic_malignancy, solid_tumor, mbp, pr, rr, bt, spo2,
                                     crp_48, wbc_48, hemoglobin_48, platelet_48, creatinine_48, bilirubin_48, antibio))
imp <- miceRanger(data1_cut, m=1, maxiter = 5,returnModels = TRUE, seed=123)
impcheck <- completeData(imp)
data2_cut<-data.frame(impcheck[1])
names(data2_cut) <- names(data1_cut)

data2<-inner_join(data2_cut, subset(data1, select= -c(group_hypo, age, sex, BMI, CCI, index_year, surgery_bfiv, sepsis, icu, drug_bfiv, 
                                                      hypertension, diabetes_mellitus, heart_failure, chronic_kidney_disease, liver_disease, cerebrovascular,
                                                      metastatic_cancer,hematologic_malignancy, solid_tumor, mbp, pr, rr, bt, spo2,
                                                      crp_48, wbc_48, hemoglobin_48, platelet_48, creatinine_48, bilirubin_48, antibio)), by='re_person_id')
#data check
#mytable(data2)
#mytable(group_hypo~.-re_person_id, data=data2)
#summary(data2)


#### step 3. Propensity Score matching
library(MatchIt)
library(ggplot2)
library(cobalt)
data2_cut$group_hypo <- ifelse(data2_cut$group_hypo=='control',0,1)

##psm : data3
matching <- matchit(group_hypo ~age+sex+BMI+CCI+index_year+surgery_bfiv+sepsis+icu+drug_bfiv+
                      hypertension+diabetes_mellitus+heart_failure+chronic_kidney_disease+
                      liver_disease+cerebrovascular+metastatic_cancer+hematologic_malignancy+
                      solid_tumor+mbp+pr+rr+bt+spo2+crp_48+wbc_48+hemoglobin_48+platelet_48+creatinine_48+bilirubin_48+antibio,
                    data=data2_cut,
                    method = "nearest",
                    distance="logit",
                    caliper=0.1,
                    seed=1234,
                    ratio=2)

#SMD check and merge
summary(matching)
love.plot(matching)
data3_cut <- match.data(matching)
data3_cut <- subset(data3_cut, select = -c(distance, weights, subclass))
data3<-inner_join(data3_cut, subset(data1, select= -c(group_hypo, age, sex, BMI, CCI, index_year, surgery_bfiv, sepsis, icu, drug_bfiv, 
                                                      hypertension, diabetes_mellitus, heart_failure, chronic_kidney_disease, liver_disease, cerebrovascular, metastatic_cancer,
                                                      hematologic_malignancy, solid_tumor, mbp, pr, rr, bt, spo2,
                                                      crp_48, wbc_48, hemoglobin_48, platelet_48, creatinine_48, bilirubin_48, antibio)), by='re_person_id')

#ps분포확인plot (psm 전)
mod <- glm(group_hypo~. -re_person_id, family=binomial, data=data2_cut)
#summary(mod)
data2_cut_plot <- data.frame(score=predict(mod, type='response'), data2_cut)
data2_cut_plot %>%
  mutate(group_hypotension=ifelse(group_hypo==0,'control','hypotension')) %>%
  ggplot(aes(x=score, fill=group_hypotension))+
  geom_histogram(color='white', position = 'dodge')+theme_bw()

#ps분포확인plot (psm 후)
mod <- glm(group_hypo~. -re_person_id, family=binomial, data=data3_cut)
#summary(mod)
data3_cut_plot <- data.frame(score=predict(mod, type='response'), data3_cut)
data3_cut_plot %>%
  mutate(group_hypotension=ifelse(group_hypo==0,'control','hypotension')) %>%
  ggplot(aes(x=score, fill=group_hypotension))+
  geom_histogram(color='white', position = 'dodge')+theme_bw()


#data check
#mytable(data3)
#mytable(group_hypo~.-re_person_id, data=data3)
#summary(data3)



####dataset save

write.csv(data1, file="E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/data1.csv")
write.csv(data2, file="E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/data2.csv")
write.csv(data3, file="E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/data3.csv")
