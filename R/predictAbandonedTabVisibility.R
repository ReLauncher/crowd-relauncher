source("R/libs/crowdResultsCollectN.R")
source("R/libs/logs_into_features.R")
library(methods)
library(sqldf)
library(caret)
library(rpart)
library(rpart.plot)
require(dplyr)
#library(ggplot)



JOB_ID <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
API_KEY <- commandArgs(trailingOnly=TRUE)[2]

results <- collectResults("CrowdFlower",API_KEY,JOB_ID,'variat', 'batch1','reward', T)

print(str(results))
currentTime <- as.numeric(Sys.time())
print(currentTime)
# setting parameters
closedWaitingTime <- 45
# derive the facts that pages/tabs are closed from pageActivity
pageActivity <- preparePageActivityDetailedLogs(JOB_ID)
#print("--------------")
#print(str(pageActivity))
#print("--------------")
pageClosed <- sqldf(paste("
  select 
    max(dt_start) as dt_start, 
    unit_id, 
    assignment_id 
  from pageActivity 
  group by unit_id, assignment_id
  having max(dt_start) < ",(currentTime-closedWaitingTime)))
#print("======================")
#print(nrow(pageClosed))
#print(str(pageClosed))
#print("======================")
#collect clickActivity logs for this task by the currentTime moment
clickActivity <- prepareMouseActivityAggregated(JOB_ID)
#filter out actions when workers Give up working on the assignment (the platform does relaunch)
clickGiveUp <- clickActivity[clickActivity$GiveUpClickedCount > 0,]

query <- paste("
            select
            tc.assignment_id,
            tc.unit_id
            from pageClosed tc
            --  left join relaunched abr on tc.assignment_id = abr.assignment_id
              left join results r on r.re_unit_id = tc.unit_id and r.re_execution_end - tc.dt_start <= 5 and r.re_execution_end - tc.dt_start > -100
              left join clickGiveUp cg on cg.assignment_id = tc.assignment_id
            where cg.assignment_id is null and r.re_unit_id is null -- and abr.assignment_id is null
            ")

new_to_relaunch <- sqldf(query)
print("=======================")
print(new_to_relaunch)
print("=======================")


fileConn<-file(paste("Datasets/ToRelaunch/",JOB_ID,".json", sep=""))
#new_to_relaunch$json <- paste('{"unit_id":',new_to_relaunch$unit_id,',"assignment_id":"',new_to_relaunch$assignment_id,'"}',sep="")
fileLines <- paste(new_to_relaunch$unit_id, collapse = ", ")
writeLines(c("[",fileLines,"]"), fileConn)
close(fileConn)