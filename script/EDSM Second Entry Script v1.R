# Target Dates ------------------------------------------------------------
date_start <- "2019-07-01"
date_end <- "2019-07-31"
target_database <- "DoubleEntry_FY2019.accdb"

# Settings ----------------------------------------------------------------
edsm_saved_table <- TRUE 
edsm_code_4 <- TRUE 
edsm_unique_output <- TRUE
edsm_first_entry_search <- FALSE
edsm_first_entry_name <- ""
edsm_second_entry_search <- FALSE
edsm_second_entry_name <- ""

# Server Information ------------------------------------------------------
sql_driver_str <- "SQL Server"
djfmp_user_id <- "djfmpreader"
djfmp_user_password <- "d1fmpR0ad3rPr0d"
first_entry_server_name <- "ifw9bct-sqlha1"


# Time --------------------------------------------------------------------
edsm_start_time=format(Sys.time(), "%Y-%m-%d_%H_%M")

# Library -----------------------------------------------------------------
if (!require('odbc')) install.packages('odbc'); library('odbc')
if (!require('dplyr')) install.packages('dplyr'); library('dplyr') 
if (!require('tidyr')) install.packages('tidyr'); library('tidyr') 
if (!require('compareDF')) install.packages('compareDF'); library('compareDF') 
if (!require('lubridate')) install.packages('lubridate'); library('lubridate') 
if (!require('devtools')) install.packages('devtools'); library('devtools') 
if (!require('here')) install.packages('here'); library('here') 


# Custom Functions --------------------------------------------------------
'%!in%' <- function(x,y)!('%in%'(x,y))

getstr = function(mystring, initial.character, final.character)
{         snippet = rep(0, length(mystring))
          for (i in 1:length(mystring))
        {
        initial.position = gregexpr(initial.character, mystring[i])[[1]][1] + 1
        final.position = gregexpr(final.character, mystring[i])[[1]][1] - 1
        snippet[i] = substr(mystring[i], initial.position, final.position)
        }
return(snippet)
}


# Open Database Connections -----------------------------------------------
first_entry_con <- odbc::dbConnect(drv=odbc::odbc(), 
                                   driver=sql_driver_str, 
                                   server=first_entry_server_name,
                                   uid=djfmp_user_id, 
                                   pwd=djfmp_user_password)

second_entry_db_string <- paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};",
                                 "Dbq=", "M:\\IEP\\rdbms\\DJFMP\\DJFMP Database\\",
                                 paste0("Current Access Applications\\",target_database))
second_entry_con <- DBI::dbConnect(drv=odbc::odbc(), 
                                   .connection_string=second_entry_db_string)


# Import Sample Data ------------------------------------------------------
first_sample_date_query <- gsub("[\r\n]"," ", sprintf("
		SELECT *
		FROM Sample
		WHERE SampleDate BETWEEN '%s' AND '%s';
", date_start, date_end))

edsm_first_sample_date_table <- DBI::dbGetQuery(conn=first_entry_con, 
                                           statement=first_sample_date_query)

second_sample_query <- "SELECT * FROM [Sample EDSM1];"

edsm_second_sample_table <- DBI::dbGetQuery(conn=second_entry_con, statement=second_sample_query)

# Wrangle Sample Data -----------------------------------------------------
edsm_first_sample_date_table<-edsm_first_sample_date_table[grepl("EDSM", edsm_first_sample_date_table$MethodCode),]
edsm_second_sample_date_table<-edsm_second_sample_table[grepl("EDSM", edsm_second_sample_table$MethodCode),]
edsm_second_sample_date_table <- edsm_second_sample_table %>% 
        filter(SampleDate >= date_start & SampleDate<=date_end)

edsm_second_sample_date_table$SampleTime <- substr(edsm_second_sample_date_table$SampleTime, 11, 20) 
edsm_second_sample_date_table$SampleDate <- as.character(edsm_second_sample_date_table$SampleDate)

edsm_first_sample_date_table<-edsm_first_sample_date_table[order(edsm_first_sample_date_table$SampleDate, edsm_first_sample_date_table$SampleTime),]
edsm_second_sample_date_table<-edsm_second_sample_date_table[order(edsm_second_sample_date_table$SampleDate, edsm_second_sample_date_table$SampleTime),]
levels(edsm_first_sample_date_table$ID) <- unique(edsm_first_sample_date_table$ID)
levels(edsm_second_sample_date_table$ID) <- unique(edsm_second_sample_date_table$ID)

# Adding ID's -------------------------------------------------------------

edsm_first_sample_date_table$ID <- paste(edsm_first_sample_date_table$StationCode,edsm_first_sample_date_table$SampleDate,edsm_first_sample_date_table$SampleTime)
edsm_first_sample_date_table$ID <- as.factor(edsm_first_sample_date_table$ID)
edsm_second_sample_date_table$ID <- paste(edsm_second_sample_date_table$StationCode,edsm_second_sample_date_table$SampleDate,edsm_second_sample_date_table$SampleTime)
edsm_second_sample_date_table$ID<-as.factor(edsm_second_sample_date_table$ID)

# Linking SampleID and ID -------------------------------------------------


edsm_first_IDs <- data.frame("StationCode"=edsm_first_sample_date_table$StationCode, "SampleDate"=edsm_first_sample_date_table$SampleDate, "SampleTime"=edsm_first_sample_date_table$SampleTime,"SampleID"=edsm_first_sample_date_table$SampleID, "edsm_first_entry"=edsm_first_sample_date_table$created_by)

edsm_second_IDs <- data.frame("StationCode"=edsm_second_sample_date_table$StationCode, "SampleDate"=edsm_second_sample_date_table$SampleDate, "SampleTime"=edsm_second_sample_date_table$SampleTime,"Sample1ID"=edsm_second_sample_date_table$SampleEDSM1ID, "edsm_second_entry"=edsm_second_sample_date_table$created_by)

edsm_first_IDs$SampleTime<-lubridate::hms(edsm_first_IDs$SampleTime)
edsm_second_IDs$SampleTime<-lubridate::hms(edsm_second_IDs$SampleTime)
edsm_first_IDs$SampleDate <- as.Date(edsm_first_IDs$SampleDate)
edsm_second_IDs$SampleDate <- as.Date(edsm_second_IDs$SampleDate)
edsm_first_IDs$SampleTime<-lubridate::hms(edsm_first_IDs$SampleTime)
edsm_second_IDs$SampleTime<-lubridate::hms(edsm_second_IDs$SampleTime)
edsm_first_IDs$ID<-paste(edsm_first_IDs$StationCode,edsm_first_IDs$SampleDate,edsm_first_IDs$SampleTime)
edsm_second_IDs$ID<-paste(edsm_second_IDs$StationCode,edsm_second_IDs$SampleDate,edsm_second_IDs$SampleTime)

edsm_first_IDs <- data.frame("StationCode"=edsm_first_sample_date_table$StationCode, "SampleDate"=edsm_first_sample_date_table$SampleDate, "SampleTime"=edsm_first_sample_date_table$SampleTime,"SampleID"=edsm_first_sample_date_table$SampleID, "first_entry"=edsm_first_sample_date_table$created_by)
edsm_second_IDs <- data.frame("StationCode"=edsm_second_sample_date_table$StationCode, "SampleDate"=edsm_second_sample_date_table$SampleDate, "SampleTime"=edsm_second_sample_date_table$SampleTime,"Sample1ID"=edsm_second_sample_date_table$SampleEDSM1ID, "second_entry"=edsm_second_sample_date_table$created_by)
#here
edsm_first_IDs$SampleTime<-lubridate::hms(edsm_first_IDs$SampleTime)
edsm_second_IDs$SampleTime<-lubridate::hms(edsm_second_IDs$SampleTime)
edsm_first_IDs$SampleDate <- as.Date(edsm_first_IDs$SampleDate)
edsm_second_IDs$SampleDate <- as.Date(edsm_second_IDs$SampleDate)
edsm_first_IDs$SampleTime<-lubridate::hms(edsm_first_IDs$SampleTime)
edsm_second_IDs$SampleTime<-lubridate::hms(edsm_second_IDs$SampleTime)
edsm_first_IDs$ID<-paste(edsm_first_IDs$StationCode,edsm_first_IDs$SampleDate,edsm_first_IDs$SampleTime)
edsm_second_IDs$ID<-paste(edsm_second_IDs$StationCode,edsm_second_IDs$SampleDate,edsm_second_IDs$SampleTime)

# Trim Unused Columns -----------------------------------------------------

edsm_first_sample_date_table<-edsm_first_sample_date_table[, intersect(colnames(edsm_second_sample_date_table), colnames(edsm_first_sample_date_table))]
edsm_second_sample_date_table<-edsm_second_sample_date_table[, intersect(colnames(edsm_first_sample_date_table), colnames(edsm_second_sample_date_table))]
edsm_first_sample_date_table$created_by <- NULL
edsm_first_sample_date_table$created_on <- NULL
edsm_first_sample_date_table$isSite <- NULL
edsm_first_sample_date_table$Username <- NULL
edsm_second_sample_date_table$created_by <- NULL
edsm_second_sample_date_table$created_on <- NULL
edsm_second_sample_date_table$isSite <- NULL
edsm_second_sample_date_table$Username <- NULL

# Compile Personnel Information ------------------------------------------
edsm_entry1 <- data.frame("ID"=edsm_first_IDs$ID, "first_entry"=edsm_first_IDs$first_entry)
edsm_entry2 <- data.frame("ID"=edsm_second_IDs$ID, "second_entry"=edsm_second_IDs$second_entry)
edsm_master_entry <- merge(edsm_entry1, edsm_entry2, by=c("ID"))
edsm_master_entry$first_entry <- getstr(edsm_master_entry$first_entry, '_', '@')
edsm_master_entry$second_entry <- tolower(edsm_master_entry$second_entry)

# Handle Code 4's ---------------------------------------------------------
if (edsm_code_4==FALSE) {edsm_first_sample_date_table <- filter(edsm_first_sample_date_table, edsm_first_sample_date_table$GearConditionCode!=4)}

if (edsm_code_4==FALSE) {edsm_second_sample_date_table <- filter(edsm_second_sample_date_table, edsm_second_sample_date_table$GearConditionCode!=4)}

# Combine, Format, and Split ----------------------------------------------
edsm_first_sample_date_table$Entry <-1
edsm_second_sample_date_table$Entry <-2
combined_table <- rbind (edsm_first_sample_date_table, edsm_second_sample_date_table)
combined_table$SampleTime<-lubridate::hms(combined_table$SampleTime)
combined_table$SampleDate <- as.Date(combined_table$SampleDate)
combined_table$SampleDate<-lubridate::ymd(combined_table$SampleDate)
combined_table$ID <- paste(combined_table$StationCode,combined_table$SampleDate,combined_table$SampleTime)
edsm_first_sample_date_table<- filter(combined_table, Entry==1)
edsm_second_sample_date_table<- filter(combined_table, Entry==2)

# Identify Orphan Samples -------------------------------------------------
edsm_first_orphan <- setdiff(edsm_first_sample_date_table$ID,edsm_second_sample_date_table$ID)
edsm_second_orphan<- setdiff(edsm_second_sample_date_table$ID, edsm_first_sample_date_table$ID)
edsm_first_sample_date_table <- filter(edsm_first_sample_date_table, edsm_first_sample_date_table$ID %!in% edsm_first_orphan)
edsm_second_sample_date_table <- filter(edsm_second_sample_date_table, edsm_second_sample_date_table$ID %!in% edsm_second_orphan)
write.csv(edsm_first_orphan, "./output/edsm_first_orphan.csv")
write.csv(edsm_second_orphan, "./output/edsm_second_orphan.csv")

edsm_first_IDs <- filter(edsm_first_IDs, edsm_first_IDs$ID %!in% edsm_first_orphan)
edsm_second_IDs <- filter(edsm_second_IDs, edsm_second_IDs$ID %!in% edsm_second_orphan)

# Add Data Entry Tech -----------------------------------------------------
edsm_first_sample_date_table<- merge(edsm_first_sample_date_table, edsm_master_entry, by="ID")
edsm_second_sample_date_table<- merge(edsm_second_sample_date_table, edsm_master_entry, by="ID")

# Import Catch Data -------------------------------------------------------
if ((file.exists("./data/edsmtable1catch.csv")) & (file.info("./data/edsmtable1catch.csv")$mtime+100000<Sys.time()) | edsm_saved_table==FALSE) {
        unlink("./data/edsmtable1catch.csv")
}
if (file.exists("./data/edsmtable1catch.csv")) {
        edsm_first_catch_table<-read.csv("./data/edsmtable1catch.csv")
} 
if (!file.exists("./data/edsmtable1catch.csv")) {
        edsm_first_catch_query <- "SELECT * FROM Catch;"
        edsm_first_catch_table <- DBI::dbGetQuery(conn=first_entry_con, statement=edsm_first_catch_query)
        write.csv(edsm_first_catch_table, "./data/edsmtable1catch.csv")
}

edsm_second_catch_query <- "SELECT * FROM CatchEDSM1;"
edsm_second_catch_table <- DBI::dbGetQuery(conn=second_entry_con, statement=edsm_second_catch_query)

# Wrangle Catch Data ------------------------------------------------------
edsm_first_catch_date_table <- filter(edsm_first_catch_table, edsm_first_catch_table$SampleID %in% edsm_first_IDs$SampleID)
edsm_second_catch_date_table <- filter(edsm_second_catch_table, edsm_second_catch_table$SampleEDSM1ID %in% edsm_second_IDs$Sample1ID)
colnames(edsm_second_IDs)[colnames(edsm_second_IDs) == 'Sample1ID'] <- 'SampleID'
colnames(edsm_second_catch_date_table)[colnames(edsm_second_catch_date_table) == 'SampleEDSM1ID'] <- 'SampleID'

edsm_first_catch_date_table$Entry<-1
edsm_second_catch_date_table$Entry<-2

edsm_first_catch_date_table<-edsm_first_catch_date_table[, intersect(colnames(edsm_second_catch_date_table), colnames(edsm_first_catch_date_table))]
edsm_second_catch_date_table<-edsm_second_catch_date_table[, intersect(colnames(edsm_first_catch_date_table), colnames(edsm_second_catch_date_table))]
edsm_first_catch_date_table<- left_join(edsm_first_catch_date_table, edsm_first_IDs, "SampleID")
edsm_second_catch_date_table<- left_join(edsm_second_catch_date_table, edsm_second_IDs, "SampleID")

edsm_first_catch_date_table <- edsm_first_catch_date_table[order(edsm_first_catch_date_table$ID),]
edsm_second_catch_date_table <- edsm_second_catch_date_table[order(edsm_second_catch_date_table$ID),]

edsm_first_catch_date_table$SampleID<-NULL
edsm_second_catch_date_table$SampleID<-NULL

edsm_first_catch_date_table$first_entry<- NULL
edsm_second_catch_date_table$second_entry<- NULL

edsm_first_catch_date_table <- merge (edsm_first_catch_date_table, edsm_master_entry, by="ID")
edsm_second_catch_date_table <- merge (edsm_second_catch_date_table, edsm_master_entry, by="ID")

# Trim Unused catch -----------------------------------------------------

edsm_first_catch_date_table<-edsm_first_catch_date_table[, intersect(colnames(edsm_second_catch_date_table), colnames(edsm_first_catch_date_table))]
edsm_second_catch_date_table<-edsm_second_catch_date_table[, intersect(colnames(edsm_first_catch_date_table), colnames(edsm_second_catch_date_table))]
edsm_first_catch_date_table$created_by <- NULL
edsm_first_catch_date_table$created_on <- NULL
edsm_first_catch_date_table$isSite <- NULL
edsm_first_catch_date_table$Username <- NULL
edsm_second_catch_date_table$created_by <- NULL
edsm_second_catch_date_table$created_on <- NULL
edsm_second_catch_date_table$isSite <- NULL
edsm_second_catch_date_table$Username <- NULL



# Constrain to Entry Tech -------------------------------------------------
edsm_first_entry_name <- tolower(edsm_first_entry_name)
edsm_second_entry_name <- tolower(edsm_second_entry_name)

if (edsm_first_entry_search == TRUE & edsm_second_entry_search ==TRUE) {
        edsm_first_sample_date_table <- filter(edsm_first_sample_date_table, first_entry==edsm_first_entry_name & second_entry== edsm_second_entry_name)       
}

if (edsm_first_entry_search== TRUE & edsm_second_entry_search== FALSE) {
        edsm_first_sample_date_table <- filter(edsm_first_sample_date_table, first_entry==edsm_first_entry_name)       
}

if (edsm_first_entry_search== FALSE & edsm_second_entry_search == TRUE) {
        edsm_first_sample_date_table <- filter(edsm_first_sample_date_table, second_entry==edsm_second_entry_name)
}

if (edsm_first_entry_search == TRUE & edsm_second_entry_search ==TRUE) {
        edsm_second_sample_date_table <- filter(edsm_second_sample_date_table, first_entry==edsm_first_entry_name & second_entry== edsm_second_entry_name)       
}

if (edsm_first_entry_search== TRUE & edsm_second_entry_search== FALSE) {
        edsm_second_sample_date_table <- filter(edsm_second_sample_date_table, first_entry==edsm_first_entry_name)       
}

if (edsm_first_entry_search== FALSE & edsm_second_entry_search == TRUE) {
        edsm_second_sample_date_table <- filter(edsm_second_sample_date_table, second_entry==edsm_second_entry_name)
}

if (edsm_first_entry_search == TRUE & edsm_second_entry_search ==TRUE) {
        edsm_first_catch_date_table <- filter(edsm_first_catch_date_table, first_entry==edsm_first_entry_name & second_entry== edsm_second_entry_name)       
}

if (edsm_first_entry_search== TRUE & edsm_second_entry_search== FALSE) {
        edsm_first_catch_date_table <- filter(edsm_first_catch_date_table, first_entry==edsm_first_entry_name)       
}

if (edsm_first_entry_search== FALSE & edsm_second_entry_search == TRUE) {
        edsm_first_catch_date_table <- filter(edsm_first_catch_date_table, second_entry==edsm_second_entry_name)
}

if (edsm_first_entry_search == TRUE & edsm_second_entry_search ==TRUE) {
edsm_second_catch_date_table <- filter(edsm_second_catch_date_table, first_entry==edsm_first_entry_name & second_entry== edsm_second_entry_name)       
}

if (edsm_first_entry_search== TRUE & edsm_second_entry_search== FALSE) {
edsm_second_catch_date_table <- filter(edsm_second_catch_date_table, first_entry==edsm_first_entry_name)       
}

if (edsm_first_entry_search== FALSE & edsm_second_entry_search == TRUE) {
edsm_second_catch_date_table <- filter(edsm_second_catch_date_table, second_entry==edsm_second_entry_name)
}

# Sample Comparison -------------------------------------------------------
edsm_sample_output<-compare_df(edsm_first_sample_date_table, edsm_second_sample_date_table, c("ID"), limit_html = 100000, tolerance = 0, tolerance_type = "difference", exclude = c("Entry", "SampleDate", "SampleTime", "StationCode", "updated_by", "updated_on","UserName"), keep_unchanged_cols = TRUE, stop_on_error = FALSE)
cat(edsm_sample_output$html_output, file="./Output/edsm_sample_comparison_results.html")
if(edsm_unique_output==FALSE){
        cat(edsm_sample_output$html_output, file="./Output/edsm_sample_comparison_results.html")
}
if (edsm_unique_output==TRUE){
        edsm_sample_comparison_file <- paste("./Output/",edsm_start_time,"edsm_sample_comparison_results.html", sep="")
        cat(edsm_sample_output$html_output, file=edsm_sample_comparison_file)
}


# Catch Comparison --------------------------------------------------------
edsm_catch_output<-compare_df(edsm_first_catch_date_table, edsm_second_catch_date_table, group_col=c("ID"), limit_html = 100000, tolerance = 0, tolerance_type = "difference", exclude = c("Entry", "TagCode", "RaceByTag", "SampleDate", "SampleTime", "StationCode"), keep_unchanged_cols= TRUE, stop_on_error = FALSE)

if(edsm_unique_output==FALSE){
cat(edsm_catch_output$html_output, file="./Output/edsm_catch_comparison_results.html")
}
if (edsm_unique_output==TRUE){
        edsm_catch_comparison_file <- paste("./Output/",edsm_start_time,"edsm_catch_comparison_results.html", sep="")
        cat(edsm_catch_output$html_output, file=edsm_catch_comparison_file)
}

### RUN COMPLETE################################################################