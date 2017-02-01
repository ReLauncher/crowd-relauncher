library(lubridate)
library(caret)
library(RCurl)
require(scales)

filterEvaluation <- function(data){
	data <- data[data$re_evaluation != -1,]
	if (nrow(data[data$re_evaluation == 2,])>0){
		data[data$re_evaluation == 2,]$re_evaluation = 1
	}
	data$re_evaluation <- as.factor(data$re_evaluation)
	data
	#data$re_evaluation_reason <- "test"
	#data$X_city <- as.factor(data$X_city)
	#data$X_channel <- as.factor(data$X_channel)
}


prepareTrainingSet <- function(data){
	training <- data[,c("re_evaluation","re_duration_num","re_execution_relative_end_num","X_channel","re_execution_relative_start_num")]
	training
}

predictEvaluation <- function(data){
	data <- filterEvaluation(data)
	training <- prepareTrainingSet(data)
	modFit <-train(re_evaluation ~ ., method="rpart", data = training)
	modFit
	#print(modFit$finalModel)
	#plot(modFit$finalModel, uniform = TRUE, main = "Classification tree")
	#text(modFit$finalModel, use.n = TRUE, all = TRUE, cex = 0.8)
}

getExportURL <- function(google_spreadsheet_url){
	paste(gsub("edit#","export?",google_spreadsheet_url),"format=csv",sep="&")
}

collectFromGoogleSpreadsheet <- function(google_spreadsheet_url){
	export_url <- getExportURL(google_spreadsheet_url)
	experiments <- read.csv(export_url,na.strings=c("NA",""))
	experiments
}
reformatExperimentData <- function(data, job_id = "0", title = "task", condition = condition){
	dumb_start_time <- as.POSIXct("07/01/2015 00:00:00", format='%m/%d/%Y %H:%M:%S')

	data$re_job_id <- job_id
	data$re_task <- title
	data$re_condition <- condition

	data$re_execution_end <- mdy_hms(data$X_created_at)
	data$re_execution_start <- mdy_hms(data$X_started_at)

	
	data$re_duration <- difftime(data$re_execution_end, data$re_execution_start, units = "secs") 
	data$re_duration_num <- as.numeric(data$re_duration)
	data$first_execution_start <- min(data$re_execution_start)
	data$re_first_execution_start <- data$first_execution_start 

	data$re_execution_relative_end <- dumb_start_time + (data$re_execution_end - data$first_execution_start)
	data$re_execution_relative_start <- data$re_execution_relative_end - data$re_duration
	
	data$re_execution_relative_start_num <- data$re_execution_relative_start - dumb_start_time
	data$re_execution_relative_end_num <- data$re_execution_relative_end - dumb_start_time

	units(data$re_execution_relative_start_num) <- "secs"
	units(data$re_execution_relative_end_num) <- "secs"

	data$re_execution_relative_start_num <- as.numeric(data$re_execution_relative_start_num)
	data$re_execution_relative_end_num <- as.numeric(data$re_execution_relative_end_num)
	
	# ============================================
	# Take only CrowdFlower or ReLauncher columns
	data <- data[,grep("re_|X_", names(data), value=TRUE)]
	data$X_unit_id <- factor(data$X_unit_id, levels = data$X_unit_id)
	data$X_worker_id <-as.character(substring(data$X_worker_id,5,8))
	data$re_unit_number <- as.numeric(rownames(data))
	data$re_unit_factor <- as.factor(rownames(data))

	data <- data[order(data$re_execution_relative_end),] 
	data$re_index <- c(1:nrow(data))

	data

}