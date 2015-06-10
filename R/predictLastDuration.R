source("R/libs/crowdResultsCollect.R")
library(rmongodb)
#library(ggplot)
#mongodb://heroku_0lq2cz6f:ookpaeqbiipb7imotbb9vt7kjr@ds043982.mongolab.com:43982/heroku_0lq2cz6f

host <- "ds043982.mongolab.com:43982"
username <- "heroku_0lq2cz6f"
password <- "ookpaeqbiipb7imotbb9vt7kjr"
db <- "heroku_0lq2cz6f"



JOB_ID <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
API_KEY <- commandArgs(trailingOnly=TRUE)[2]
UNITS_AMOUNT <- as.numeric(commandArgs(trailingOnly=TRUE)[3])

print(UNITS_AMOUNT)
data <- collectResults("CrowdFlower",API_KEY,JOB_ID,'variat', 'batch1','reward', T)
if (data != FALSE && nrow(data)>=5){
	data <- data[order(data$execution_relative_end),] 

	x <- data$duration_num
	print(x)
	indexes <- c(1:nrow(data))
	#filename = paste('Public/',JOB_ID,'.pdf',sep="")

	#pdf(filename)
	#plot(indexes, x, xlab="Judgement index",ylab="Judgement duration, seconds", xlim=c(1,max(c(UNITS_AMOUNT,nrow(data)))))

	i <-1

	current_slope <- 0
	slopes <- c()
	y <-c()
	y[1] <- x[1]
	y_indexes <-c(1)
	while(i < length(x)){
		i<- i+1
		if (x[i]>y[length(y_indexes)]){
			y_indexes[length(y_indexes)+1]<- i
			y[length(y_indexes)] <- x[i]
		}
	}

	#points(y_indexes, y, col='red')

	hypothesis1.lm = lm(y ~ y_indexes)
	print(summary(hypothesis1.lm))
	current_slope <-hypothesis1.lm$coefficients['y_indexes']

	current_prediction <- (max(c(UNITS_AMOUNT,nrow(data)))*current_slope)+hypothesis1.lm$coefficients['(Intercept)']
	current_prediction <- round(current_prediction)
	print(current_prediction)

	#abline(hypothesis1.lm, col=alpha('grey', 0.5))
	#abline(current_prediction,0, col=alpha('blue', 0.5))
	#dev.off()
	
	completed <- nrow(data)
}else{
	current_prediction <- 999999 
	completed <- 0
}

fileConn<-file(paste("Datasets/Limits/",JOB_ID,".txt"))
json <- paste('{"job_id":',JOB_ID,', "limit":',current_prediction,', "completed":',completed,'}', sep='')
writeLines(c(json), fileConn)
close(fileConn)

#m <- mongo.create(host=host , db=db, username=username, password=password)
#ns <- "admin.collection"
#bson <- mongo.bson.from.JSON(json)
#mongo.insert(m, ns, bson)

