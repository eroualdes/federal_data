(ns federal-data.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clj-http.client :as http])
  (:gen-class))

(def MSHA
  {:agency :msha,
   :home "http://www.msha.gov/STATS/PART50/p50y2k/p50y2k.HTM",
   :data "http://www.msha.gov/OpenGovernmentData/OGIMSHA.asp",
   :accident_injury "http://www.msha.gov/OpenGovernmentData/DataSets/Accidents.zip",
   :area "http://www.msha.gov/OpenGovernmentData/DataSets/AreaSamples.zip",
   :coal_dust "http://www.msha.gov/OpenGovernmentData/DataSets/CoalDustSamples.zip",
   :conferenes "http://www.msha.gov/OpenGovernmentData/DataSets/Conferences.zip",
   :civil_penalty "http://www.msha.gov/OpenGovernmentData/DataSets/CivilPenaltyDocketsAndDecisions.zip",
   :employment_production "http://www.msha.gov/OpenGovernmentData/DataSets/MinesProdQuarterly.zip",
   :inspections "http://www.msha.gov/OpenGovernmentData/DataSets/Inspections.zip",
   :mine_addresses "http://www.msha.gov/OpenGovernmentData/DataSets/AddressofRecord.zip",
   :mines "http://www.msha.gov/OpenGovernmentData/DataSets/Mines.zip",
   :noise "http://www.msha.gov/OpenGovernmentData/DataSets/NoiseSamples.zip",
   :health "http://www.msha.gov/OpenGovernmentData/DataSets/PersonalHealthSamples.zip",
   :quartz "http://www.msha.gov/OpenGovernmentData/DataSets/QuartzSamples.zip",
   :violations "http://www.msha.gov/OpenGovernmentData/DataSets/Violations.zip",
   :contested_violations "http://www.msha.gov/OpenGovernmentData/DataSets/ContestedViolations.zip",
   :assessed_violations "http://www.msha.gov/OpenGovernmentData/DataSets/AssessedViolations.zip"})

(def BLS
  {:agency :bls,
   :home "http://www.bls.gov",
   :data "http://www.bls.gov/cew/datatoc.htm",
   :docs "http://www.bls.gov/cew/doctoc.htm",
   :naics #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_naics10_totals.zip",
   :industry #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_by_industry.zip"})

;;; utilities
(defn copy [uri file]
  (with-open [in (io/input-stream uri)
              out (io/output-stream file)]
    (io/copy in out)))

;;; download
(defmulti download :agency)

(defmethod download :msha [agency fldr]
  (let [urls (vals (dissoc agency [:agency :home :data]))
        names (map #(.getName %) (map io/file urls))
        files (map io/file (repeat fldr) names)]
    (pmap copy urls files)
    (shutdown-agents)))

(defmethod download :bls [agency fldr]
  (let [html_content ((http/get (agency :data)) :body)
        naics (re-seq (agency :naics) html_content)
        indstry (re-seq (agency :industry) html_content)
        urls_suf (into (map first naics) (map first indstry))
        names (map #(.getName %) (map io/file urls_suf))
        files (map io/file (repeat fldr) names)
        urls (map #(str (agency :home) %) urls_suf)]
    (pmap copy urls files)
    (shutdown-agents)))

;;; main
(def cli-options
  [
   ["-h" "--help" "print this message and exit"]
   ["-a" "--agency AGENCY" "acronym of agency of interest"
    :parse-fn #(read-string %)]
   ["-f" "--folder FOLDER" "folder to store agency data, absolute path"]])

(defn usage [options-summary]
  (->> ["federal_data => analysis ready\n"
        "Usage: java -jar federal_data.jar -a AGENCY -f FOLDER action\n"
        "Arguments:"
        options-summary
        "\nActions:"
        "  download\tdownload agency data"
        "  transform\ttransform downloaded agency data"
        "\nSee README for more information."]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ;; Handle help and error conditions
    (cond
     (options :help) (exit 0 (usage summary))
     errors (exit 1 (error-msg errors))
     ;; (not (and (options :agency) (options :folder))) (exit 1 (error-msg errors))
     )
    (println options)
    (println arguments)))
