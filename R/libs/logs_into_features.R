source("R/libs/collect_experiments.R")

dumb_start_time <- as.POSIXct("07/01/2020 00:00:00", format='%m/%d/%Y %H:%M:%S')

LogsFolder <- "Datasets/Logs/"
prepareUnitResults <- function(JOB_ID, TASK_TYPE, GOOGLE_SPREADSHEET_URL, filter_bad_input = TRUE){
	experiment <- collectFromGoogleSpreadsheet(GOOGLE_SPREADSHEET_URL)
	experiment <- reformatExperimentData(experiment,JOB_ID,TASK_TYPE,0)
	if (filter_bad_input){
		experiment <- filterEvaluation(experiment)
	}

	experiment
}
preparePageActivityDetailedLogs <- function(JOB_ID, break_time = dumb_start_time){
	p_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_page.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	p_data$dt_start <- as.POSIXct(as.numeric(p_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	p_data$dt_end <- as.POSIXct(as.numeric(p_data$dt_end)/1000, origin="1970-01-01",tz="UTC")

	p_data$task_id <- as.factor(p_data$task_id)
	p_data$unit_id <- as.factor(p_data$unit_id)
	#p_data$assignment_id <- as.factor(p_data$assignment_id)
	p_data$session_id <- as.factor(p_data$session_id)
	p_data <- p_data[order(p_data$assignment_id,p_data$dt_start),]

	p_data <- p_data[p_data$dt_start <= as.numeric(break_time),]
	p_data
}
preparePageActivityAggregated <- function(JOB_ID, break_time = dumb_start_time){
	p_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_page.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	p_data$dt_start <- as.POSIXct(as.numeric(p_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	p_data$dt_end <- as.POSIXct(as.numeric(p_data$dt_end)/1000, origin="1970-01-01",tz="UTC")

	p_data$task_id <- as.factor(p_data$task_id)
	p_data$unit_id <- as.factor(p_data$unit_id)
	p_data$session_id <- as.factor(p_data$session_id)

	page_activity <- sqldf(paste("\r
		select
			p.task_id,
			p.unit_id,
			p.assignment_id,
			sum(p.keyboard) as keyboard, 
			sum(p.mouse) as mouse, 
			sum(p.scroll) as scroll, 
			sum(p.text_selected) as text_selected, 
			count(p.keyboard) as amount, \r
			
			sum(case when p.scroll >= 0 and p.scroll <= 0.1*ps.page_length then 1 else 0 end)*2 as SawScreenPart1, \r
			sum(case when p.scroll > 0.1*ps.page_length and p.scroll <= 0.2*ps.page_length then 1 else 0 end)*2 as SawScreenPart2, \r
			sum(case when p.scroll > 0.2*ps.page_length and p.scroll <= 0.3*ps.page_length then 1 else 0 end)*2 as SawScreenPart3, \r
			sum(case when p.scroll > 0.3*ps.page_length and p.scroll <= 0.4*ps.page_length then 1 else 0 end)*2 as SawScreenPart4, \r
			sum(case when p.scroll > 0.4*ps.page_length and p.scroll <= 0.5*ps.page_length then 1 else 0 end)*2 as SawScreenPart5, \r
			sum(case when p.scroll > 0.5*ps.page_length and p.scroll <= 0.6*ps.page_length then 1 else 0 end)*2 as SawScreenPart6, \r
			sum(case when p.scroll > 0.6*ps.page_length and p.scroll <= 0.7*ps.page_length then 1 else 0 end)*2 as SawScreenPart7, \r
			sum(case when p.scroll > 0.7*ps.page_length and p.scroll <= 0.8*ps.page_length then 1 else 0 end)*2 as SawScreenPart8, \r
			sum(case when p.scroll > 0.8*ps.page_length and p.scroll <= 0.9*ps.page_length then 1 else 0 end)*2 as SawScreenPart9, \r
			sum(case when p.scroll > 0.9*ps.page_length and p.scroll <= 1.0*ps.page_length then 1 else 0 end)*2 as SawScreenPart10 \r
		\r
		from p_data p \r
		left join (select \r
			unit_id, assignment_id, min(dt_start) as session_l, max(dt_end) as session_r, (max(dt_end) - min(dt_start))/10 as block_size,\r
			cast(max(scroll) as float) as page_length
		from p_data \r
		where dt_start < ",as.numeric(break_time)," \r
		group by unit_id, assignment_id) ps on p.unit_id = ps.unit_id and p.assignment_id = ps.assignment_id   \r 
		where p.dt_start < ",as.numeric(break_time)," \r
		group by p.task_id, p.unit_id, p.assignment_id",sep=""))

	page_activity <- page_activity[page_activity$unit_id != "No data available",]
	page_activity <- page_activity[page_activity$unit_id != "Na",]
	page_activity
}

prepareTabActivityLogs <- function(JOB_ID, break_time = dumb_start_time){
	t_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_tabs.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	
	t_data$dt_start <- as.POSIXct(as.numeric(t_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	t_data$dt_end <- as.POSIXct(as.numeric(t_data$dt_end)/1000, origin="1970-01-01",tz="UTC")
	t_data$status_duration <-round(as.numeric(difftime(t_data$dt_end,t_data$dt_start, units = "secs")))
	t_data$task_id <- as.factor(t_data$task_id)
	t_data$unit_id <- as.factor(t_data$unit_id)
	t_data$session_id <- as.factor(t_data$session_id)
	t_data[t_data$status==" opened","status"] <- as.factor(" active")
	t_data$min_start_time <- min(t_data$dt_start)

	query <- paste("\r
		select \r
		t_data.*,
			round(dt_start - min_start_time,0) as dt_start_relative, 
			round(dt_end - min_start_time,0) as dt_end_relative
		from t_data \r
		where dt_start < ",as.numeric(break_time),"
		",sep="")
	tabs_activity <- sqldf(query)

	tabs_activity
}
prepareTabActivityAggregated <- function(JOB_ID, break_time = dumb_start_time){
	tabActivity <- prepareTabActivityLogs(JOB_ID, break_time)
	query <- paste("
		select 
			sum(case when status = ' active' then status_duration else 0 end) as active_duration,
			sum(case when status = ' hidden' then status_duration else 0 end) as hidden_duration,
			sum(case when status = ' closed' then 1 else 0 end) as closed_count,
			count(distinct session_id) as sessions_count,
			assignment_id
		from tabActivity
		where dt_start < ",as.numeric(break_time),"
		group by assignment_id
		")
	tabActivityAggregated <- sqldf(query)
	tabActivityAggregated
}
prepareKeysActivityAggregated <- function(JOB_ID, break_time = dumb_start_time){
	k_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_keys.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	k_data$dt_start <- as.POSIXct(as.numeric(k_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	k_data$task_id <- as.factor(k_data$task_id)
	k_data$unit_id <- as.factor(k_data$unit_id)
	k_data$session_id <- as.factor(k_data$session_id)
	k_data$key <- as.factor(k_data$key)
	k_data$waiting <- 30
	k_data <- k_data[order(k_data$session_id,k_data$dt_start),]


	for(i in seq(from=2, to=nrow(k_data), by=1)){
		if (k_data[i,'session_id'] == k_data[i-1,'session_id']){
			k_data[i,'waiting'] <- k_data[i,'dt_start'] - k_data[i-1,'dt_start']
			}else{
				k_data[i,'waiting'] <- 0
			}
	}

	#t_data[t_data$status_duration<0,c("unit_id","assignment_id","session_id","status","status_duration")]
	key_activity <- sqldf(paste("\r
		select \r
			min(dt_start) as key_first_stroke, \r
			max(waiting) as key_waiting_max, \r
			SQRT(AVG(waiting*waiting) - AVG(waiting)*AVG(waiting)) as key_waiting_sd, \r
			AVG(waiting) as key_waiting_mean, \r
			sum(case when waiting > 2 then 1 else 0 end) as KeyStrokesCount, \r
			sum(case when waiting > 2 then waiting else 0 end) as InterStrokesTime, \r
			\r
			sum(case when key in (8,46) then 1 else 0 end) as key_delete, \r
			sum(case when key in (9) then 1 else 0 end) as key_tab, \r
			sum(case when key in (13) then 1 else 0 end) as key_enter, \r
			sum(case when key in (16) then 1 else 0 end) as key_shift, \r
			sum(case when key in (17) then 1 else 0 end) as key_cntrl, \r
			sum(case when key in (18) then 1 else 0 end) as key_alt, \r
			sum(case when key in (19) then 1 else 0 end) as key_pause, \r
			sum(case when key in (20) then 1 else 0 end) as key_caps, \r
			sum(case when key in (27) then 1 else 0 end) as key_esc, \r
			sum(case when key in (33) then 1 else 0 end) as key_page_up, \r
			sum(case when key in (34) then 1 else 0 end) as key_page_down, \r
			sum(case when key in (35) then 1 else 0 end) as key_end, \r
			sum(case when key in (36) then 1 else 0 end) as key_home, \r
			sum(case when key in (37) then 1 else 0 end) as key_left, \r
			sum(case when key in (38) then 1 else 0 end) as key_up, \r
			sum(case when key in (39) then 1 else 0 end) as key_right, \r
			sum(case when key in (40) then 1 else 0 end) as key_down, \r
			sum(case when key in (45) then 1 else 0 end) as key_insert, \r
			sum(case when key >=48 and key <= 57 then 1 else 0 end) as key_digit, \r
			sum(case when key >=65 and key <= 90 then 1 else 0 end) as key_char, \r
			count(distinct key) as key_unique, \r
			\r
			count(key) as key_all, \r
			task_id,unit_id,assignment_id \r
		from k_data \r
		where dt_start < ",as.numeric(break_time)," \r
		group by task_id, unit_id, assignment_id \r
		",sep=""))
	key_activity
}

prepareMouseActivityLogs <- function(JOB_ID, break_time = dumb_start_time){
	t_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_clicks.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	t_data$dt_start <- as.POSIXct(as.numeric(t_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	t_data$task_id <- as.factor(t_data$task_id)
	t_data$unit_id <- as.factor(t_data$unit_id)
	t_data$session_id <- as.factor(t_data$session_id)
	t_data$element <- as.factor(t_data$element)

	t_data[t_data$dt_start < as.numeric(break_time),]
	t_data
}

prepareMouseActivityAggregated <- function(JOB_ID, break_time = dumb_start_time){
	k_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_clicks.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	k_data$dt_start <- as.POSIXct(as.numeric(k_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	k_data$task_id <- as.factor(k_data$task_id)
	k_data$unit_id <- as.factor(k_data$unit_id)
	k_data$session_id <- as.factor(k_data$session_id)
	k_data$element <- as.factor(k_data$element)
	
	#t_data[t_data$status_duration<0,c("unit_id","assignment_id","session_id","status","status_duration")]
	mouseclick_activity <- sqldf(paste("
		select
			count(element) as click_all,
			count(distinct element) as ElementsClicked, 
			count(distinct case when element like '%INPUT%' then element else 0 end)-1 as FieldsAccessedCount,
			sum(case when element like '%#give-up-modal > DIV:nth-child(3) > A:nth-child(2)%' then 1 else 0 end) as GiveUpClickedCount,
			task_id,unit_id,assignment_id 
		from k_data 
		where dt_start < ",as.numeric(break_time),"
		group by task_id, unit_id, assignment_id
		",sep=""))
	mouseclick_activity
}
prepareAssignments <- function(JOB_ID, break_time = dumb_start_time){
	# ---------------------------------------
	# BASED ON TABS ACTIVITY
	t_data <- read.table(paste(LogsFolder,JOB_ID,"/",JOB_ID,"_page.csv",sep=""), header=T, sep="," ,quote = "\"", comment.char = "")
	t_data$dt_start <- as.POSIXct(as.numeric(t_data$dt_start)/1000, origin="1970-01-01",tz="UTC")
	t_data$dt_end <- as.POSIXct(as.numeric(t_data$dt_end)/1000, origin="1970-01-01",tz="UTC")
	t_data$status_duration <-round(as.numeric(difftime(t_data$dt_end,t_data$dt_start, units = "secs")))
	t_data$task_id <- as.factor(t_data$task_id)
	t_data$unit_id <- as.factor(t_data$unit_id)
	t_data$session_id <- as.factor(t_data$session_id)
	#t_data[t_data$status==" opened","status"] <- as.factor(" active")
	# ---------------------------------------
	query <- paste("\r
		select d.*, case when d.dt_end = a.dt then 0 else 1 end as abandoned from \r
		(select task_id, unit_id, assignment_id, min(dt_start) as dt_start, max(dt_start) as dt_end, max(dt_start) - min(dt_start) as duration \r
			from t_data where dt_start < '",break_time,"' group by task_id, unit_id, assignment_id ) d \r
		inner join (select \r 
		task_id,unit_id, max(dt_start) as dt \r
		from t_data where dt_start < ",as.numeric(break_time)," \r
		group by task_id, unit_id) a on d.unit_id = a.unit_id",sep="")
	assignments <- sqldf(query)

	assignments
}

prepareEvaluationTrainingSet <- function(job_dataset){
	eva_set <- job_dataset
	eva_set <- filter(eva_set, re_evaluation!=-10)
	eva_set <- select(eva_set, -(abandoned))
	eva_set$re_evaluation <- as.factor(eva_set$re_evaluation) 
	
	eva_set
}

prepareAbandonenceTrainingSet <- function(job_dataset){
	aband_set <- job_dataset
	aband_set$abandoned <- as.factor(aband_set$abandoned)
	aband_set <- aband_set[,names(aband_set)!="re_evaluation"]
	aband_set
}
buildModel <- function(training_set, label_is_evaluation = T, method = "rpart"){
	if (label_is_evaluation){
		fit <- train(re_evaluation ~ ., method = method, data = training_set, control = rpart.control(minsplit = 3))
	}
	else{
		fit <- train(re_abandoned ~ ., method = method, data = training_set, control = rpart.control(minsplit = 3))
	}
	fit
}

drawDecisionTree <- function(fit,filename, title = "Decision tree"){
	pdf(paste("Predictions/",filename,".pdf",sep=""), width=6, height=3)
	prp(fit, title = "Accuracy", extra = 1)
	dev.off()
}

predictTaskAccuracy <- function(task_training, task_test){
	training_all <- prepareEvaluationTrainingSet(task_training)
	testing <- prepareEvaluationTrainingSet(task_test) #eval_train[- seq(1,border_index),]
	
	pred <- data.frame(alpha=NA, sensitivity=NA, specificity=NA, accuracy=NA)[numeric(), ]

	size <- nrow(training_all)
	for(i in seq(from=0.05, to=1.0, by=0.05)){
		border_index <- floor(size * i)
		
		training <- training_all[seq(1,border_index),]
		if (nrow(training[training$re_evaluation < 0,]) >1 && nrow(training[training$re_evaluation > 0,]) >1 ) {
			
			#testing <- eval_train[- seq(1,border_index),]
			eval_fit <- buildModel(training, T)
			print(eval_fit$finalModel)
			predictions <- predict(eval_fit, testing)

			cf <- confusionMatrix(predictions, testing$re_evaluation)

			measurement <- c(i,nrow(training),nrow(testing),cf$table[1,1],cf$table[1,2],cf$table[2,1],cf$table[2,2],round(as.numeric(cf$overall["Accuracy"]),2),round(as.numeric(cf$byClass['Specificity']),2),round(as.numeric(cf$byClass['Sensitivity']),2))
			pred <- rbind(pred,measurement)
		}
		
	}
	colnames(pred) <- c("alpha","train_size","test_size","tn","fn","fp","tp","accuracy","specificity","sensitivity")
	pred
}


prepareAssignmentFeatures <- function(JOB_ID, break_time = dumb_start_time){
	page_activity_aggr <- preparePageActivityAggregated(JOB_ID, break_time)
	tab_activity_aggr <- prepareTabActivityAggregated(JOB_ID, break_time)
	key_activity_aggr <- prepareKeysActivityAggregated(JOB_ID, break_time)
	mouse_activity_aggr <- prepareMouseActivityAggregated(JOB_ID, break_time)
	assignments <- prepareAssignments(JOB_ID, break_time)
	
	featuresDataset <- sqldf("\r
		select \r
			kall.key_first_stroke - a.dt_start as BeforeTypingDelay,
			kall.KeyStrokesCount as KeyStrokesCount,
			kall.InterStrokesTime as InterStrokesTime,
			kall.key_waiting_mean as KeyWaitingMean,
			kall.key_waiting_sd as KeyWaitingSD,
			IFNULL(kall.key_delete,0) as KeyDeleteCount,
			IFNULL(kall.key_tab,0) as KeyTabCount,
			IFNULL(kall.key_enter,0) as KeyEnterCount,
			IFNULL(kall.key_shift,0) as KeyShiftCount,
			IFNULL(kall.key_cntrl,0) as KeyCNTRLCount,
			IFNULL(kall.key_alt,0) as KeyAltCount,
			IFNULL(kall.key_insert,0) as KeyInsertCount,
			IFNULL(kall.key_unique,0) as UniqueKeysCount,
			IFNULL(kall.key_all,0) as AllKeysCount,
			IFNULL(mall.click_all,0) as ClickEventCount,
			IFNULL(mall.ElementsClicked,0) as ElementsClicked,
			IFNULL(mall.FieldsAccessedCount,0) as FieldsAccessedCount,
			IFNULL(mall.GiveUpClickedCount,0) as GiveUpClickedCount,
			p.SawScreenPart1 as SawScreenPart1,
			p.SawScreenPart2 as SawScreenPart2,
			p.SawScreenPart3 as SawScreenPart3,
			p.SawScreenPart4 as SawScreenPart4,
			p.SawScreenPart5 as SawScreenPart5,
			p.SawScreenPart6 as SawScreenPart6,
			p.SawScreenPart7 as SawScreenPart7,
			p.SawScreenPart8 as SawScreenPart8,
			p.SawScreenPart9 as SawScreenPart9,
			p.SawScreenPart10 as SawScreenPart10,
			t.sessions_count as SessionsCount,
			t.hidden_duration as TabWasHidden,
			t.closed_count as TabWasClosed,
			a.dt_end - a.dt_start as asnmt_duration,
			a.dt_start as assignment_start,
			a.dt_end as assignment_end,
			a.assignment_id as assignment_id,
			a.unit_id as unit_id
		from assignments a
			left join page_activity_aggr p on a.assignment_id = p.assignment_id 
			left join tab_activity_aggr t on a.assignment_id = t.assignment_id
			left join key_activity_aggr kall on a.assignment_id = kall.assignment_id
			left join mouse_activity_aggr mall on a.assignment_id = mall.assignment_id
		group by a.assignment_id,a.unit_id 
		")
	
	featuresDataset <- sqldf("\r
		select \r
			IFNULL(BeforeTypingDelay,asnmt_duration) as BeforeTypingDelay,\r
			IFNULL(InterStrokesTime/KeyStrokesCount,0) as MeanInterStrokeTime, \r
			IFNULL(KeyStrokesCount,0) as KeyStrokesCount, \r
			IFNULL(KeyWaitingMean,0) as WithinTypingDelay, \r
			IFNULL(KeyDeleteCount,0) as KeyDeleteCount, \r
			IFNULL(KeyTabCount,0) as KeyTabCount, \r
			IFNULL(KeyEnterCount,0) as KeyEnterCount, \r
			IFNULL(KeyShiftCount,0) as KeyShiftCount, \r
			IFNULL(KeyCNTRLCount,0) as KeyCNTRLCount, \r
			IFNULL(KeyAltCount,0) as KeyAltCount, \r
			IFNULL(KeyInsertCount,0) as KeyInsertCount, \r
			IFNULL(UniqueKeysCount/AllKeysCount,0) as UniqueKeysPressed, \r
			IFNULL(AllKeysCount,0) as KeyboardEventCount, \r
			SawScreenPart1 as SawScreenPart1, \r
			SawScreenPart2 as SawScreenPart2, \r
			SawScreenPart3 as SawScreenPart3, \r
			SawScreenPart4 as SawScreenPart4, \r
			SawScreenPart5 as SawScreenPart5, \r
			SawScreenPart6 as SawScreenPart6, \r
			SawScreenPart7 as SawScreenPart7, \r
			SawScreenPart8 as SawScreenPart8, \r
			SawScreenPart9 as SawScreenPart9, \r
			SawScreenPart10 as SawScreenPart10,
			ClickEventCount,
			ElementsClicked,
			FieldsAccessedCount,
			GiveUpClickedCount,
			SessionsCount,
			TabWasHidden,
			TabWasClosed,
			assignment_start,
			assignment_end,
			asnmt_duration as TotalTime,
			assignment_id,
			unit_id
		from featuresDataset
		")
	featuresDataset$BeforeTypingDelay <- as.numeric(featuresDataset$BeforeTypingDelay)
	featuresDataset$MeanInterStrokeTime <- as.numeric(featuresDataset$MeanInterStrokeTime)
	featuresDataset$KeyStrokesCount <- as.numeric(featuresDataset$KeyStrokesCount)
	featuresDataset$WithinTypingDelay <- as.numeric(featuresDataset$WithinTypingDelay)
	# featuresDataset$KeyWaitingSD <- as.numeric(featuresDataset$KeyWaitingSD)
	featuresDataset$KeyDeleteCount <- as.numeric(featuresDataset$KeyDeleteCount)
	# featuresDataset$KeyTabCount <- as.numeric(featuresDataset$KeyTabCount)
	featuresDataset$KeyEnterCount <- as.numeric(featuresDataset$KeyEnterCount)
	featuresDataset$KeyShiftCount <- as.numeric(featuresDataset$KeyShiftCount)
	featuresDataset$KeyCNTRLCount <- as.numeric(featuresDataset$KeyCNTRLCount)
	featuresDataset$KeyAltCount <- as.numeric(featuresDataset$KeyAltCount)
	featuresDataset$KeyInsertCount <- as.numeric(featuresDataset$KeyInsertCount)
	featuresDataset$UniqueKeysPressed <- as.numeric(featuresDataset$UniqueKeysPressed)
	
	featuresDataset$TabWasHidden <- as.numeric(featuresDataset$TabWasHidden)
	featuresDataset$TabWasClosed <- as.numeric(featuresDataset$TabWasClosed)
	featuresDataset$SessionsCount <- as.numeric(featuresDataset$SessionsCount)

	featuresDataset$ClickEventCount <- as.numeric(featuresDataset$ClickEventCount)
	featuresDataset$ElementsClicked <- as.numeric(featuresDataset$ElementsClicked)
	featuresDataset$FieldsAccessedCount <- as.numeric(featuresDataset$FieldsAccessedCount)
	featuresDataset$GiveUpClickedCount <- as.numeric(featuresDataset$GiveUpClickedCount)
	
	featuresDataset$SawScreenPart1 <- as.numeric(featuresDataset$SawScreenPart1)
	featuresDataset$SawScreenPart2 <- as.numeric(featuresDataset$SawScreenPart2)
	featuresDataset$SawScreenPart3 <- as.numeric(featuresDataset$SawScreenPart3)
	featuresDataset$SawScreenPart4 <- as.numeric(featuresDataset$SawScreenPart4)
	featuresDataset$SawScreenPart5 <- as.numeric(featuresDataset$SawScreenPart5)
	featuresDataset$SawScreenPart6 <- as.numeric(featuresDataset$SawScreenPart6)
	featuresDataset$SawScreenPart7 <- as.numeric(featuresDataset$SawScreenPart7)
	featuresDataset$SawScreenPart8 <- as.numeric(featuresDataset$SawScreenPart8)
	featuresDataset$SawScreenPart9 <- as.numeric(featuresDataset$SawScreenPart9)
	featuresDataset$SawScreenPart10 <- as.numeric(featuresDataset$SawScreenPart10)

	featuresDataset
}
prepareFeaturesAssignmentsDataset <- function(JOB_ID, TASK_TYPE, GOOGLE_SPREADSHEET_URL, break_time = dumb_start_time){
	units <- prepareUnitResults(JOB_ID, TASK_TYPE, GOOGLE_SPREADSHEET_URL)
	page_activity <- preparePageActivityAggregated(JOB_ID, break_time)
	tabs_activity <- prepareTabActivityLogs(JOB_ID, break_time)
	key_activity <- prepareKeysActivityAggregated(JOB_ID, break_time)
	mouse_activity <- prepareMouseActivityAggregated(JOB_ID, break_time)
	assignments <- prepareAssignments(JOB_ID, break_time)
	
	
	#		a.abandoned,\r 
	#		and a.abandoned = 0 \r
	featuresDataset <- sqldf("\r
		select \r
			min(kall.key_first_stroke) - min(a.dt_start) as BeforeTypingDelay, \r
			sum(kall.KeyStrokesCount) as KeyStrokesCount, \r
			sum(kall.InterStrokesTime) as InterStrokesTime, \r
			min(kall.key_waiting_mean) as KeyWaitingMean, \r
			min(kall.key_waiting_sd) as KeyWaitingSD, \r
			sum(IFNULL(kall.key_delete,0)) as KeyDeleteCount, \r
			sum(IFNULL(kall.key_tab,0)) as KeyTabCount, \r
			sum(IFNULL(kall.key_enter,0)) as KeyEnterCount, \r
			sum(IFNULL(kall.key_shift,0)) as KeyShiftCount, \r
			sum(IFNULL(kall.key_cntrl,0)) as KeyCNTRLCount, \r
			sum(IFNULL(kall.key_alt,0)) as KeyAltCount, \r
			sum(IFNULL(kall.key_insert,0)) as KeyInsertCount, \r
			sum(IFNULL(kall.key_unique,0)) as UniqueKeysCount, \r
			sum(IFNULL(kall.key_all,0)) as AllKeysCount, \r
			\r
			sum(IFNULL(mall.click_all,0)) as ClickEventCount, \r
			sum(IFNULL(mall.ElementsClicked,0)) as ElementsClicked, \r
			
			max(IFNULL(FieldsAccessedCount,0)) as FieldsAccessedCount, \r
			\r
			sum(p.SawScreenPart1) as SawScreenPart1, \r
			sum(p.SawScreenPart2) as SawScreenPart2, \r
			sum(p.SawScreenPart3) as SawScreenPart3, \r
			sum(p.SawScreenPart4) as SawScreenPart4, \r
			sum(p.SawScreenPart5) as SawScreenPart5, \r
			sum(p.SawScreenPart6) as SawScreenPart6, \r
			sum(p.SawScreenPart7) as SawScreenPart7, \r
			sum(p.SawScreenPart8) as SawScreenPart8, \r
			sum(p.SawScreenPart9) as SawScreenPart9, \r
			sum(p.SawScreenPart10) as SawScreenPart10, \r
			\r
			count(distinct ta.session_id) as SessionsCount, \r
			IFNULL(th.status_duration*1.0/(ta.status_duration+th.status_duration),0) as TabWasHidden, \r
			count(tc.assignment_id) as TabWasClosed, \r
			round(case when e.re_duration_num is null then max(a.dt_end) - min(a.dt_start) else e.re_duration_num end) as asnmt_duration, \r
			\r
			max(a.abandoned) as abandoned, \r
			min(a.dt_start) as assignment_start, \r
			max(a.dt_end) as assignment_end, \r 
			max(e.re_execution_relative_end) as re_execution_relative_end, \r
			a.assignment_id as assignment_id, \r
			case when e.re_evaluation not null then e.re_evaluation else -10 end as re_evaluation \r
		from assignments a\r
			left join units e on abandoned = 0 and a.unit_id = e.re_unit_id
			left join page_activity p on a.unit_id = p.unit_id and a.assignment_id = p.assignment_id 
			left join tabs_activity ta on a.unit_id = ta.unit_id and a.assignment_id = ta.assignment_id and ta.status like '%active%'
			left join tabs_activity th on a.unit_id = th.unit_id and a.assignment_id = th.assignment_id and th.status like '%hidden%'
			left join tabs_activity tc on a.unit_id = tc.unit_id and a.assignment_id = tc.assignment_id and tc.status like '%closed%'
			left join key_activity kall on a.unit_id = kall.unit_id and a.assignment_id = kall.assignment_id
			left join mouse_activity mall on a.unit_id = mall.unit_id and a.assignment_id = mall.assignment_id
		where \r
		 	ta.unit_id is not null and p.unit_id is not null \r
		group by e.re_duration_num, a.assignment_id, case when e.re_evaluation not null then e.re_evaluation else -10 end \r
		order by e.re_execution_relative_end \r
		")
	
	featuresDataset <- sqldf("\r
		select \r
			IFNULL(BeforeTypingDelay,asnmt_duration) as BeforeTypingDelay,\r
			IFNULL(InterStrokesTime/KeyStrokesCount,0) as MeanInterStrokeTime, \r
			IFNULL(KeyStrokesCount,0) as KeyStrokesCount, \r
			IFNULL(KeyWaitingMean,0) as WithinTypingDelay, \r
			-- IFNULL(KeyWaitingSD,0) as KeyWaitingSD, \r
			IFNULL(KeyDeleteCount,0) as KeyDeleteCount, \r
			-- IFNULL(KeyTabCount,0) as KeyTabCount, \r
			IFNULL(KeyEnterCount,0) as KeyEnterCount, \r
			IFNULL(KeyShiftCount,0) as KeyShiftCount, \r
			IFNULL(KeyCNTRLCount,0) as KeyCNTRLCount, \r
			IFNULL(KeyAltCount,0) as KeyAltCount, \r
			IFNULL(KeyInsertCount,0) as KeyInsertCount, \r
			IFNULL(UniqueKeysCount/AllKeysCount,0) as UniqueKeysPressed, \r
			-- IFNULL(AllKeysCount,0) as KeyboardEventCount, \r
			SawScreenPart1 as SawScreenPart1, \r
			SawScreenPart2 as SawScreenPart2, \r
			SawScreenPart3 as SawScreenPart3, \r
			SawScreenPart4 as SawScreenPart4, \r
			SawScreenPart5 as SawScreenPart5, \r
			SawScreenPart6 as SawScreenPart6, \r
			SawScreenPart7 as SawScreenPart7, \r
			SawScreenPart8 as SawScreenPart8, \r
			SawScreenPart9 as SawScreenPart9, \r
			SawScreenPart10 as SawScreenPart10, \r
			ClickEventCount, \r
			ElementsClicked, \r
			FieldsAccessedCount, \r
			SessionsCount, \r
			TabWasHidden, \r
			TabWasClosed, \r
			abandoned, \r
			assignment_start, \r
			assignment_end, \r
			asnmt_duration as TotalTime, \r
			re_execution_relative_end, \r
			assignment_id, \r
			re_evaluation
		from featuresDataset
		")
	featuresDataset$BeforeTypingDelay <- as.numeric(featuresDataset$BeforeTypingDelay)
	featuresDataset$MeanInterStrokeTime <- as.numeric(featuresDataset$MeanInterStrokeTime)
	featuresDataset$KeyStrokesCount <- as.numeric(featuresDataset$KeyStrokesCount)
	featuresDataset$WithinTypingDelay <- as.numeric(featuresDataset$WithinTypingDelay)
	# featuresDataset$KeyWaitingSD <- as.numeric(featuresDataset$KeyWaitingSD)
	featuresDataset$KeyDeleteCount <- as.numeric(featuresDataset$KeyDeleteCount)
	# featuresDataset$KeyTabCount <- as.numeric(featuresDataset$KeyTabCount)
	featuresDataset$KeyEnterCount <- as.numeric(featuresDataset$KeyEnterCount)
	featuresDataset$KeyShiftCount <- as.numeric(featuresDataset$KeyShiftCount)
	featuresDataset$KeyCNTRLCount <- as.numeric(featuresDataset$KeyCNTRLCount)
	featuresDataset$KeyAltCount <- as.numeric(featuresDataset$KeyAltCount)
	featuresDataset$KeyInsertCount <- as.numeric(featuresDataset$KeyInsertCount)
	featuresDataset$UniqueKeysPressed <- as.numeric(featuresDataset$UniqueKeysPressed)
	
	featuresDataset$TabWasHidden <- as.numeric(featuresDataset$TabWasHidden)
	featuresDataset$TabWasClosed <- as.numeric(featuresDataset$TabWasClosed)
	featuresDataset$SessionsCount <- as.numeric(featuresDataset$SessionsCount)

	featuresDataset$ClickEventCount <- as.numeric(featuresDataset$ClickEventCount)
	featuresDataset$ElementsClicked <- as.numeric(featuresDataset$ElementsClicked)
	featuresDataset$FieldsAccessedCount <- as.numeric(featuresDataset$FieldsAccessedCount)
	
	featuresDataset$SawScreenPart1 <- as.numeric(featuresDataset$SawScreenPart1)
	featuresDataset$SawScreenPart2 <- as.numeric(featuresDataset$SawScreenPart2)
	featuresDataset$SawScreenPart3 <- as.numeric(featuresDataset$SawScreenPart3)
	featuresDataset$SawScreenPart4 <- as.numeric(featuresDataset$SawScreenPart4)
	featuresDataset$SawScreenPart5 <- as.numeric(featuresDataset$SawScreenPart5)
	featuresDataset$SawScreenPart6 <- as.numeric(featuresDataset$SawScreenPart6)
	featuresDataset$SawScreenPart7 <- as.numeric(featuresDataset$SawScreenPart7)
	featuresDataset$SawScreenPart8 <- as.numeric(featuresDataset$SawScreenPart8)
	featuresDataset$SawScreenPart9 <- as.numeric(featuresDataset$SawScreenPart9)
	featuresDataset$SawScreenPart10 <- as.numeric(featuresDataset$SawScreenPart10)

	featuresDataset

}
