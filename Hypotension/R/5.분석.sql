####이 코드는 R을 통해 데이터를 분석(survival analysis, cox proportional hazards model, subgroup ananlysis)을 진행합니다.
#### step info
#### step 1. survival analysis
#### step 2. cox proportional hazards model
#### step 3. subgroup ananlysis

#install.package("survival")
#install.package("survminer")
#install.package("moonBook")
#install.package("forestploter")


#data3 <- read_csv('E:/Users/DKKI002/Desktop/mimic/baseline table_cdm_202407/data3.csv', col_names = T)

#### step 1. survival analysis
library(survival) #생존분석
library(survminer) #생존곡선

## Kaplan-Meier 생존분석
surv <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')

#로그-순위 검정 확인
survdiff(surv ~ group_hypo, data=data3)

#Suvival probabillity plot
surv_fit <- survfit(surv ~ group_hypo, data=data3)
ggsurvplot(surv_fit,
           #fun='pct',
           pval=T,
           pval.coord=c(0, 0),
           risk.table = T ,
           conf.int = T,
           conf.int.alpha=0.1,
           xlab='Time, d',
           legend.title="Group",
           legend.labs=c("Control", "Hypotension"),
           legend= c(0.9, 0.9), 
           break.x.by=10, break.y.by=0.2, #표시간격
           ggtheme=theme_bw(),
           palette='Set1')



#### step 2. cox proportional hazards model
surv <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')

#model1(group_hypo+age+sex)
fit.coxph1 <- coxph(surv ~ group_hypo+age+sex, data=as.data.frame(data3))
summary(fit.coxph1)
ggforest(fit.coxph1)

#model2(group_hypo+age+sex+sepsis+cci_group)
fit.coxph <- coxph(surv ~ group_hypo+age+sex+sepsis+cci_group, data=as.data.frame(data3))
summary(fit.coxph)
ggforest(fit.coxph)




#### step 3. subgroup ananlysis
##각각의 subgroup으로 나누어 hazard ratio를 확인하여 forest plot을 그림
##subgroup.csv에 Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value, interaction P를 입력
##값이 모두 채워진 subgroup.csv를 통해 forest plot을 그림
library(moonBook)

##data 준비
##subgroup : group_hypotension
#group_hypotension : All patients에 기입
surv <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
mytable(group_hypo~., data=subset(data3,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
fit.coxph <- coxph(surv ~ group_hypo, data=as.data.frame(data3))
summary(fit.coxph)
ggforest(fit.coxph)

##subgroup : age
#age : age>=에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
data3[data3$age >= 65, "age_g"]="age>=65"
subgroup <- subset(data3, age_g=="age>=65")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#age : age<에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
data3[data3$age < 65, "age_g"]="age<65"
subgroup <- subset(data3, age_g=="age<65")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#age : age interaction P에 기입(group_hypohypotension:age_gage>=65의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*age_g+age_g+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


##subgroup : sex
#sex : Male에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, sex=="M")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#sex : Female에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, sex=="F")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#age : age interaction P에 기입(group_hypohypotension:sexM의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*sex+sex+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


##subgroup : CCI
#CCI : cci>2에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
data3[data3$CCI > 2, "cci_g"]="cci>2"
subgroup <- subset(data3, cci_g=="cci>2")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#CCI : cci<=2에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
data3[data3$CCI <= 2, "cci_g"]="cci<=2"
subgroup <- subset(data3, cci_g=="cci<=2")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#CCI : CCI interaction P에 기입(group_hypohypotension:cci_gcci>2의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*cci_g+cci_g+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


##subgroup : sepsis
#sepsis : yes에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, sepsis=="yes")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#sepsis : no에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, sepsis=="no")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#sepsis : sepsis interaction P에 기입(group_hypohypotension:sepsisyes의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*sepsis+sepsis+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


##subgroup : Used Vasopressin before Index IV
#Used Vasopressin before Index IV : yes에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, drug_bfiv=="yes")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#Used Vasopressin before Index IV : no에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, drug_bfiv=="no")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#Used Vasopressin before Index IV : Used Vasopressin before Index IV interaction P에 기입(group_hypohypotension:drug_bfivyes의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*drug_bfiv+drug_bfiv+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


##subgroup : Index IV in ICU
#Index IV in ICU : yes에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, icu=="yes")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#Index IV in ICU : no에 기입(Hypotension N, Control N, Hazard Ratio, Lower .95, Upper .95, P-value)
subgroup <- subset(data3, icu=="no")
mytable(group_hypo~., data=subset(subgroup,select = c(group_hypo,dead_hosp)))#in hospital mortality 확인 
surv_sub <- Surv(subgroup$drug_to_end, subgroup$dead_hosp=='yes')
fit.coxph <- coxph(surv_sub ~ group_hypo, data=as.data.frame(subgroup))
ggforest(fit.coxph)

#Index IV in ICU : Index IV in ICU interaction P에 기입(group_hypohypotension:icuyes의 Pr(>|z|))
surv_sub <- Surv(data3$drug_to_end, data3$dead_hosp=='yes')
fit.coxph <- coxph(surv ~ group_hypo*icu+icu+group_hypo, data=as.data.frame(data3))
summary(fit.coxph)


#save result -> subgroup.csv
library(forestploter)
sub<- read_csv('subgroup.csv', col_names = T)

sub$Hypotension <- ifelse(is.na(sub$Hypotension),"",sub$Hypotension)
sub$Control <- ifelse(is.na(sub$Control),"",sub$Control)
sub$' '<- paste(rep(" ",40), collapse = " ")
sub$'HR (95% CI)' <- ifelse(is.na(sub$HR), "", sprintf("%.2f (%.2f to %.2f)", sub$HR, sub$L, sub$U))
sub$Subgroup <- ifelse(sub$Hypotension=="", sub$Subgroup, paste0("    ", sub$Subgroup))
sub$'P-value' <- ifelse(is.na(sub$'P-value')," ", sub$'P-value')
sub$'Interaction P' <- ifelse(is.na(sub$'Interaction P')," ", sub$'Interaction P')

tm <- forest_theme(core=list(bg_params=list(fill = c("white"))))
forest(data=sub[, c(1:3,9,7:8)],
       #base_size=300,
       est = sub$HR,
       lower= sub$L,
       upper= sub$U,
       ci_column = 4,
       
       ref_line = 1,
       xlab = "Hazard Ratio",
       xlim = c(0.75,3),
       ticks_at = c(0.75, 1,1.25, 1.5, 1.75,2,2.5,3),
       #boxsize=2,
       ci_pch=15,
       theme = tm
)