#####################################################################
# HCOMP 15 - Micro task time execution
#####################################################################

# download required libraries
# install.packages('downloader')
# install.packages('iterators')
# install.packages('lubridate')

library(downloader)
library(lubridate)
library(scales)

dumb_start_time <- as.POSIXct("01/01/2015 00:00:00", format='%m/%d/%Y %H:%M:%S')

collectResults <- function(crowdsourcing_platform, api_key, job_id, title, batch,variation, download = F){
	# depending on the crowdsourcing platform the parsing of files is different
	if (crowdsourcing_platform == "CrowdFlower"){
		data <-collectResultsCrowdFlower(job_id, api_key, title, batch,variation, download = F)
	}
	if (data != FALSE){
		# compliment the dataset with extra data
	data$job_id <- job_id
	data$task <- title
	data$batch <- batch
	data$variation <- variation
	# difftime helps to definitely define the timedifference in seconds
	data$duration <- difftime(data$re_execution_end, data$re_execution_start, units = "secs") 
	data$platform <- crowdsourcing_platform

	data$first_execution_start <- min(data$re_execution_start)
	
	# store unit indexes
	data$execution_relative_end <- dumb_start_time + (data$re_execution_end - data$first_execution_start)
	# in order to preserve the order
	data$duration_num <- as.numeric(data$duration)
	# bug fix - if the duration is in minutes and not seconds
	#data[data$duration_num<10,]$duration_num<-data[data$duration_num<10,]$duration_num*60

	data$execution_relative_start <- data$execution_relative_end - data$duration
	data$execution_relative_start_num <- as.numeric(data$execution_relative_start - dumb_start_time)
	data$execution_relative_end_num <- as.numeric(data$execution_relative_end - dumb_start_time)

	
	#workers <- as.numeric(rownames(unique(data$worker_id))
	#data$worker_name <- workers[match(data$worker_id,workers$worker_id),"worker_id"]

	data <- data[,c("re_unit_id","worker_id","job_id","duration","re_execution_end","re_execution_start","first_execution_start","execution_relative_end","execution_relative_start","duration_num","execution_relative_start_num","execution_relative_end_num")]
	
	}
	data
}
collectResultsCrowdFlower <- function(job_id, api_key, title, batch,variation, download = F){

	# (comment if is already downloaded)download the latest zip file with full results for the target job
	con <- download(paste("https://api.crowdflower.com/v1/jobs/",job_id,".csv?type=full&key=",api_key, sep = ""), mode = "wb", destfile = paste("Datasets/",job_id,".zip", sep=""))
	
	# read the csv from the zip file
	data <- read.table(unz(paste("Datasets/",job_id,".zip", sep=""), paste("f",job_id,".csv", sep="")), header=T, sep=",",quote = "\"",comment.char = "")
	if (nrow(data) == 0){
		result = FALSE
	}else{
		#units <- read.table(unz(paste("output/",batch,"/source",job_id,".csv.zip", sep=""), paste("source",job_id,".csv", sep="")), header=T, sep=",",quote = "\"",comment.char = "")
		#units$deployment_time <- mdy_hms(units$X_created_at)
		#units <- units[,c("X_unit_id","deployment_time")]

		#data <- merge(data, units, by.x = "X_unit_id", by.y = "X_unit_id")

		# parse string/factor columns into date format
		#data$result <- paste(data$address, data$company_name, data$date,data$total)
		data$re_execution_end <- mdy_hms(data$X_created_at)
		data$re_execution_start <- mdy_hms(data$X_started_at)
		data$re_unit_id <- factor(data$X_unit_id)
		#data$input <- data$image_url
		data$worker_id <-as.character(substring(data$X_worker_id,5,8))
		result = data
	}
	
	closeAllConnections()
	result

}