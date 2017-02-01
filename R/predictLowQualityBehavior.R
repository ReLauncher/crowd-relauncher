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
modelTrainedPath <- "R/Models/ImageLabeling_854885_5_100.rds"

#collect clickActivity logs for this task by the currentTime moment
clickActivity <- prepareMouseActivityAggregated(JOB_ID)
#filter out actions when workers Give up working on the assignment (the platform does relaunch)
clickGiveUp <- clickActivity[clickActivity$GiveUpClickedCount > 0,]

# collect current assignment_logs
features_for_all_assignments <- prepareAssignmentFeatures(JOB_ID)
features_for_all_assignments <- sqldf("
select f.*
from features_for_all_assignments f
  inner join (select max(assignment_end) as assignment_end, unit_id from features_for_all_assignments group by unit_id) m on f.unit_id = m.unit_id and f.assignment_end = m.assignment_end
")
# remove assignments which already were relaunched
features_for_not_relaunched <- features_for_all_assignments
# features_for_not_relaunched <- features_for_all_assignments[!(features_for_all_assignments$assignment_id %in% relaunched$assignment_id),]
# remove assignments which were not completed
features_for_not_relaunched <- features_for_not_relaunched[(features_for_not_relaunched$unit_id %in% results$re_unit_id),]
# remove assignments where workers clicked Give Up button:
features_for_not_relaunched <- features_for_not_relaunched[!(features_for_not_relaunched$assignment_id %in% clickGiveUp$assignment_id),]

# retrieve a model
modelTrainedAccuracy <- readRDS(modelTrainedPath)
# predict assignments to be relaunched in the current data using the model
predictions <- predict(modelTrainedAccuracy, features_for_not_relaunched)
new_to_relaunch <- features_for_not_relaunched[predictions == 0,]
# return new relaunched assignments
new_to_relaunch


fileConn<-file(paste("Datasets/ToRelaunch/",JOB_ID,".json", sep=""))
#new_to_relaunch$json <- paste('{"unit_id":',new_to_relaunch$unit_id,',"assignment_id":"',new_to_relaunch$assignment_id,'"}',sep="")
fileLines <- paste(new_to_relaunch$unit_id, collapse = ", ")
writeLines(c("[",fileLines,"]"), fileConn)
close(fileConn)