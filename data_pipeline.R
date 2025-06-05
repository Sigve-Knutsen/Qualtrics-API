# Combined Data Pipeline: Fetch and Clean Survey Data

# Load necessary libraries
library(tidyverse)
library(haven)
library(janitor)
library(lubridate)
library(httr)
library(jsonlite)
library(glue)

# =============================================================================
# DATA FETCHING FROM QUALTRICS
# =============================================================================

cat("Starting data pipeline...\n")

# Qualtrics API configuration
token       <- ""
datacenter  <- "eu"
survey_id   <- ""
base_url    <- glue("https://{datacenter}.qualtrics.com/API/v3")

# Create data folder if it doesn't exist
data_folder <- "data"
if (!dir.exists(data_folder)) {
  dir.create(data_folder)
  cat("Created data folder\n")
}

# Check if data already exists
data_file <- file.path(data_folder, "survey_data.sav")
if (file.exists(data_file)) {
  cat("Survey data file already exists. Skipping download.\n")
} else {
  cat("Fetching data from Qualtrics...\n")
  
  # 1) Start export job
  start <- POST(
    url         = glue("{base_url}/surveys/{survey_id}/export-responses"),
    add_headers(
      "X-API-TOKEN"   = token,
      "Content-Type"  = "application/json"
    ),
    body = list(
      format       = "spss",
      useLabels    = TRUE,
      breakoutSets = FALSE
    ),
    encode = "json"
  )
  
  progress_id <- content(start)$result$progressId
  cat("Export job started with progress ID:", progress_id, "\n")
  
  # 2) Poll progress
  cat("Waiting for export to complete...")
  repeat {
    prog <- GET(
      glue("{base_url}/surveys/{survey_id}/export-responses/{progress_id}"),
      add_headers("X-API-TOKEN" = token)
    )
    status <- content(prog)$result$status
    cat(".")
    if (status == "complete") {
      file_id <- content(prog)$result$fileId
      cat(" Complete!\n")
      break
    }
    Sys.sleep(3)
  }
  
  # 3) Download zip
  zip_path <- file.path(data_folder, "qualtrics_export.zip")
  GET(
    glue("{base_url}/surveys/{survey_id}/export-responses/{file_id}/file"),
    add_headers("X-API-TOKEN" = token),
    write_disk(zip_path, overwrite = TRUE)
  )
  cat("Data downloaded successfully\n")
  
  # 4) Extract .sav and clean up
  unzip(zip_path, exdir = data_folder)
  sav_files <- list.files(data_folder, pattern = "\\.sav$", full.names = TRUE)
  
  if (length(sav_files) > 0) {
    if (length(sav_files) > 1) {
      message("Warning: Multiple SPSS files found. Using the first one: ", sav_files[1])
    }
    
    # Get the first .sav file
    sav_file <- sav_files[1]
    
    if (sav_file != data_file) {
      file.rename(sav_file, data_file)
      cat("SPSS file renamed to: survey_data.sav\n")
    }
  } else {
    stop("No SPSS file found in the extracted files")
  }
  
  # Clean up ZIP file
  if (file.exists(zip_path)) {
    file.remove(zip_path)
    cat("Cleaned up temporary files\n")
  }
}


# DATA CLEANING AND PROCESSING

cat("Starting data cleaning...\n")

# Import and clean the data
data_survey <- read_sav(data_file) %>%
  clean_names() %>% 
  rename_with(~ str_replace(.x, "qid", "q")) %>% 
  filter(!is.na(distribution_channel)) %>%
  select(-matches("^ip|^recipient|external|location")) %>% 
  mutate(treatment = factor(case_when(
    scenario == "scandal_1" ~ "Scandal1",
    scenario == "scandal_1_police" ~ "Scandal1_Police",
    scenario == "scandal_1_guidance" ~ "Scandal1_Guidance",
    scenario == "scandal_1_strict" ~ "Scandal1_Strict", 
    scenario == "scandal_2" ~ "Scandal2",
    scenario == "control_group" ~ "Control",
    TRUE ~ "Other")),
    svartid = seconds_to_period(duration_in_seconds)) %>%
  # Remove records categorized as "Other" as they contain empty answers
  filter(treatment != "Other")

cat("Initial data loaded with", nrow(data_survey), "observations\n")

# Create unlabelled version of data for analysis
data_survey_lab <- data_survey %>% 
  mutate(across(where( ~ haven::is.labelled(.)), ~ labelled::unlabelled(.)))

# Create a numeric version of the data
data_survey_numeric <- data_survey_lab %>%
  mutate(across(matches("q12|q19|q15|q16|q18|q20|q52|q53|q56|q58"), as.numeric))

# Define column groups for analysis
trust_cols <- paste0("q12_trust_", 1:8)
tax_morality_cols <- paste0("q19_tax_moral_", 1:6)
fairness_cols <- paste0("q55_fairness_proce_", 1:5)
detection_cols <- c("q15_audit_chance", "q16_detection_chan")
penalty_cols <- c("q52_penalty_chance", "q53_penalty_sever", "q58_penalty_fair")
social_norms_cols <- c("q56_evasion_share", "q58_accept_share")
general_trust_cols <- c("q20_institut_trust_1", "q20_institut_trust_2", "q20_institut_trust_3", 
                        "q20_institut_trust_5", "q20_institut_trust_7", 
                        "q20_institut_trust_8")

# Create comprehensive question labels for all scales
question_labels <- list(
  Tillit = list(
    q12_trust_1 = "lytter til det jeg sier",
    q12_trust_2 = "er åpen om feil de gjør",
    q12_trust_3 = "behandler alle likt",
    q12_trust_4 = "har kompetanse til å gjøre jobben sin",
    q12_trust_5 = "arbeider effektivt",
    q12_trust_6 = "tar vare på rettighetene mine",
    q12_trust_7 = "prioriterer riktig",
    q12_trust_8 = "tar avgjørelser på riktig måte"
  ),
  
  Skattemoral = list(
    q19_tax_moral_1 = "Fordi jeg er redd for straff hvis jeg ikke gjør det",
    q19_tax_moral_2 = "Fordi Skatteetaten vil oppdage det hvis jeg ikke gjør det",
    q19_tax_moral_3 = "Fordi det er min plikt som samfunnsborger",
    q19_tax_moral_4 = "Fordi jeg tror sjansen for å bli kontrollert er stor",
    q19_tax_moral_5 = "Fordi jeg vil bidra til samfunnet",
    q19_tax_moral_6 = "Fordi det er det rette å gjøre"
  ),
  
  Rettferdig_behandling = list(
    q55_fairness_proce_1 = "Skatteetaten behandler meg rettferdig",
    q55_fairness_proce_2 = "Reglene og prosedyrene som Skatteetaten følger er like for alle",
    q55_fairness_proce_3 = "Skatteetaten er objektiv og upartisk",
    q55_fairness_proce_4 = "Skatteetaten forebygger feil",
    q55_fairness_proce_5 = "Skatteetaten retter opp feil når de oppdages"
  ),
  
  Detection = list(
    q15_audit_chance = "Sannsynlighet for kontroll ved unndragelse",
    q16_detection_chan = "Sannsynlighet for at Skatteetaten oppdager unndragelse ved kontroll"
  ),
  
  Penalty = list(
    q52_penalty_chance = "Sannsynlighet for straff ved oppdaget unndragelse",
    q53_penalty_sever = "Hvor alvorlig straffen oppfattes",
    q58_penalty_fair = "Hvor rettferdig straffen oppfattes"
  ),
  
  Social_Norms = list(
    q56_evasion_share = "Antatt andel som unndrar skatt",
    q58_accept_share = "Antatt andel som mener det er akseptabelt å unndra skatt"
  ),
  
  General_Trust = list(
    q20_institut_trust_1 = "Stortinget",
    q20_institut_trust_2 = "Rettsvesenet",
    q20_institut_trust_3 = "NAV",
    q20_institut_trust_5 = "Politiet",
    q20_institut_trust_7 = "Arbeidstilsynet",
    q20_institut_trust_8 = "Norske nyhetsmedier"
  )
)

# Add fairness question labels if fairness columns exist
if(length(fairness_cols) > 0) {
  question_labels$Rettferdig_behandling <- setNames(
    map_chr(
      fairness_cols,
      ~ {
        label <- attributes(data_survey_lab[[.x]])$label
        if(!is.null(label)) {
          str_remove_all(label, ".* - |\\.\\s?$")
        } else {
          .x  # fallback to column name if no label
        }
      }
    ),
    fairness_cols
  )
  cat("Fairness columns found and labeled:", length(fairness_cols), "variables\n")
} else {
  cat("No fairness columns (q55) found in dataset\n")
}

# Ensure all variable groups are numeric
data_survey_numeric <- data_survey_numeric %>%
  mutate(
    across(all_of(c(fairness_cols, detection_cols, penalty_cols, 
                    social_norms_cols, general_trust_cols)), as.numeric)
  )

