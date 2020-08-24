##run thursday 

#dbWriteTable(con, SQL("sctemp.IB_actuals"), data_agg)

library(DBI)
con <- dbConnect(odbc::odbc(), "RP_SDDC_Archive", uid = "BMafarj1", 
                 pwd = "Mafarjeh20#", timeout = 10)

data_new<-dbGetQuery(con,"SELECT trn.wh_id, t.trlr_id, trn.trndte, trn.supnum,
decode(trn.actcod, 'PUT_UNDR', trn.frstol, trn.tostol) trknum, trn.prtnum, trn.lotnum,
SUM(decode(trn.actcod, 'PUT_UNDR', 1, 0)) lpns_received,
SUM(decode(trn.actcod, 'PUT_UNDR', 0, 1)) lpns_reversed,
SUM(decode(trn.actcod, 'PUT_UNDR', 1, -1)) lpns_total,
sum(decode(trn.actcod, 'PUT_UNDR', trn.trnqty, 0)) received,
sum(decode(trn.actcod, 'PUT_UNDR', 0, trn.trnqty)) reversed,
sum(decode(trn.actcod, 'PUT_UNDR', trn.trnqty, -trn.trnqty)) total
FROM ARCWMP.dlytrn trn
LEFT JOIN ARCWMP.rcvtrk t
ON trn.wh_id = t.wh_id
AND t.trknum = decode(trn.actcod, 'PUT_UNDR', trn.frstol, trn.tostol)
WHERE ((trn.actcod = 'PUT_UNDR'
        AND   trn.fr_arecod = 'EXPR')
       OR  (trn.actcod = 'RVRCP'
            AND   trn.to_arecod = 'EXPR'))
AND trn.trndte Between TO_DATE(TO_CHAR(SYSDATE-8, 'yyyy-mon-dd'), 'yyyy-mon-dd')
		and TO_DATE(TO_CHAR(SYSDATE-1, 'yyyy-mon-dd'), 'yyyy-mon-dd')
GROUP BY trn.wh_id, t.trlr_id, trn.trndte, trn.supnum, decode(trn.actcod, 'PUT_UNDR', trn.frstol, trn.tostol), trn.prtnum, trn.lotnum")

library(lubridate)

data_new$date<-date(data_new$TRNDTE)

data_agg<- aggregate(x = data_new[c("LPNS_RECEIVED")],
                     FUN = sum,
                     by = list(Group.date = data_new$date,Dc=data_new$WH_ID))

con <- dbConnect(odbc::odbc(), "Azure_test", uid = "sacondiscuser", 
                 pwd = "Ax#4qiZV#PQR", timeout = 10)

dbAppendTable(con, SQL("sctemp.IB_actuals"), data_agg) 

#data_agg