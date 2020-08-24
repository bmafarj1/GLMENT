# Main Packages
library(tidyverse)
library(lubridate)
library(tidyquant)
library(plotly)
# Time Based Feature Extraction
library(timetk)
# Holidays
library(timeDate)
# Forecasting
library(forecast)
library(sweep)
# Machine Learning 
library(parsnip)
library(yardstick)
#DATA IMPORT

library(DBI)
con <- dbConnect(odbc::odbc(), "Azure_test", uid = "sacondiscuser", 
                 pwd = "Ax#4qiZV#PQR", timeout = 10) 
ib<-dbGetQuery(con,"select * from sctemp.IB_actuals")


##us13
us13<-ib[ib$Dc=="US13",]
us13<-us13%>% select(Group.date,LPNS_RECEIVED)
names(us13)<-c("date","cnt")
data_tbl <- us13 %>%
  tk_augment_timeseries_signature() 

data_tbl <- data_tbl %>% 
  select(date, cnt, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)

names(data_tbl)

holidays <- holidayNYSE(year = c(2018, 2019,2020)) %>% ymd()

data_tbl <- data_tbl %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))

library(glmnet)
glmnet_fit <- linear_reg(mode = "regression", penalty = 0.01, mixture = 0.5) %>%
  set_engine("glmnet") %>%
  fit.model_spec(cnt ~ . - date, data = data_tbl)

date<-c(Sys.Date() + c(11:17))
date<-as.data.frame(date)

date<-date%>%tk_augment_timeseries_signature()

locked_week <- date %>% 
  select(date, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)


locked_week <- locked_week %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))


fcast_glmnet_13 <- glmnet_fit %>%
  predict(new_data = locked_week)

rownames(fcast_glmnet_13 )<-date$date
names(fcast_glmnet_13)<-c('us13')
############ us16
us16<-ib[ib$Dc=="US16",]

us16<-ib[ib$Dc=="US16",]
us16<-us16%>% select(Group.date,LPNS_RECEIVED)
names(us16)<-c("date","cnt")
data_tbl <- us16 %>%
  tk_augment_timeseries_signature() 

data_tbl <- data_tbl %>% 
  select(date, cnt, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)

names(data_tbl)

holidays <- holidayNYSE(year = c(2018, 2019,2020)) %>% ymd()

data_tbl <- data_tbl %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))

library(glmnet)
glmnet_fit <- linear_reg(mode = "regression", penalty = 0.01, mixture = 0.5) %>%
  set_engine("glmnet") %>%
  fit.model_spec(cnt ~ . - date, data = data_tbl)

date<-c(Sys.Date() + c(11:17))
date<-as.data.frame(date)

date<-date%>%tk_augment_timeseries_signature()

locked_week <- date %>% 
  select(date, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)


locked_week <- locked_week %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))


fcast_glmnet_16 <- glmnet_fit %>%
  predict(new_data = locked_week)

rownames(fcast_glmnet_16)<-date$date
names(fcast_glmnet_16)<-c('us16')
############ us19
us19<-ib[ib$Dc=="US19",]


us19<-ib[ib$Dc=="US19",]

us19<-ib[ib$Dc=="US19",]
us19<-us19%>% select(Group.date,LPNS_RECEIVED)
names(us19)<-c("date","cnt")
data_tbl <- us19 %>%
  tk_augment_timeseries_signature() 

data_tbl <- data_tbl %>% 
  select(date, cnt, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)

names(data_tbl)

holidays <- holidayNYSE(year = c(2018, 2019,2020)) %>% ymd()

data_tbl <- data_tbl %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))

library(glmnet)
glmnet_fit <- linear_reg(mode = "regression", penalty = 0.01, mixture = 0.5) %>%
  set_engine("glmnet") %>%
  fit.model_spec(cnt ~ . - date, data = data_tbl)

date<-c(Sys.Date() + c(11:17))
date<-as.data.frame(date)

date<-date%>%tk_augment_timeseries_signature()

locked_week <- date %>% 
  select(date, year, half, quarter,mweek,qday,week, month.lbl, day, wday.lbl)


locked_week <- locked_week %>%
  mutate(holiday = case_when(
    date %in% holidays ~ 1,
    TRUE ~ 0
  ))


fcast_glmnet_19 <- glmnet_fit %>%
  predict(new_data = locked_week)

rownames(fcast_glmnet_19)<-date$date
names(fcast_glmnet_19)<-c('us19')



output<-as.data.frame(cbind(fcast_glmnet_13,fcast_glmnet_16,fcast_glmnet_19))

output$network<-(output$us13+output$us16+output$us19)
output$date<-rownames(output)
rownames(output)<-NULL



#dbWriteTable(con, SQL("sctemp.IB_forecasts"), output)
dbAppendTable(con, SQL("sctemp.IB_forecasts"), output) 

dbGetQuery(con,"select * from sctemp.IB_forecasts")
