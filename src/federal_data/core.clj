; This file is part of federal_data.
; 
; federal_data is free software: you can redistribute it and/or modify
; it under the terms of the GNU General Public License as published by
; the Free Software Foundation, either version 3 of the License, or
; (at your option) any later version.
; 
; federal_data is distributed in the hope that it will be useful,
; but WITHOUT ANY WARRANTY; without even the implied warranty of
; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
; GNU General Public License for more details.
; 
; You should have received a copy of the GNU General Public License
; along with federal_data.  If not, see <http://www.gnu.org/licenses/>.

(ns federal-data.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [org.httpkit.client :as http]
            [miner.ftp :as ftp])
  (:gen-class))
(set! *warn-on-reflection* true)

;;; agencies
(def MSHA {:agency :msha,
   :home "http://www.msha.gov/STATS/PART50/p50y2k/p50y2k.HTM",
   :data-main "http://www.msha.gov/OpenGovernmentData/OGIMSHA.asp",
   :docs "http://www.msha.gov/STATS/PART50/P50Y2K/P50Y2KHB.PDF",
   :data {:accident_injury "http://www.msha.gov/OpenGovernmentData/DataSets/Accidents.zip",
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
          :assessed_violations "http://www.msha.gov/OpenGovernmentData/DataSets/AssessedViolations.zip"}})

(def BLS {:agency :bls,
          :home "http://www.bls.gov",
          :docs "http://www.bls.gov/cew/doctoc.htm",
          :data-main "http://www.bls.gov/cew/datatoc.htm",
          :data {:naics #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_naics10_totals.zip",
                 :industry #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_by_industry.zip"}})

(def NOAA {:agency :noaa,
           :home "http://www.ncdc.noaa.gov/data-access",
           :data-main "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/",
           :client "ftp://anonymous:pwd@ftp.ncdc.noaa.gov/pub/data/ghcn/daily",
           :docs "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt",
           :data {:all "ghcnd_all.tar.gz",
                  :gsn "ghcnd_gsn.tar.gz",
                  :hcn "ghcnd_hcn.tar.gz",
                  :inventory "ghcnd-inventory.txt",
                  :states "ghcnd-states.txt",
                  :stations "ghcnd-stations.txt",
                  :countires "ghcnd-countries.txt"}})

(def EPA {:agency :epa,
          :home "http://www.epa.gov/airquality/airdata/index.html",
          :docs "http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/FileFormats.html",
          :data-main "http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html",
          :data-base "http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/"
          :data {:gases {:ozone #"hourly_44201_(\d{4}).zip",
                         :so2 #"hourly_42401_(\d{4}).zip",
                         :co #"hourly_42101_(\d{4}).zip",
                         :no2 #"hourly_42602_(\d{4}).zip"}
                 :meterological {:wind #"hourly_WIND_(\d{4}).zip",
                                 :temp #"hourly_TEMP_(\d{4}).zip",
                                 :pressure #"hourly_PRESS_(\d{4}).zip",
                                 :dewpoint #"hourly_RH_DP_(\d{4}).zip"}}})

(def agencies
  {"MSHA" MSHA, "BLS" BLS, "NOAA" NOAA, "EPA" EPA})

;;; utilities
(defn available
  ([]
     (let [ags (string/join ", " (keys agencies))]
      (println "federal_data from the following agencies:" ags)))
  ([agency]
     (if (nil? agency)
       (available)
       (let [ag (agencies agency)
             datasets (string/join ", " (map name (-> ag :data keys)))]
         (println agency "contains data sets:" datasets)))))

(defn prep-urls
  "Scan url of agencies with regexes for their :data urls to fill in
  regexes.  Return a list of domainless urls."
  [url regs]
  (let [content (:body @(-> url http/get))]
    (loop [r regs
           result []]
      (if (empty? r)
        result
        (recur (rest r)
               (into result (map first (re-seq (first r) content))))))))

(defn copy [from to name]
  (with-open [in (io/input-stream from)
              out (io/output-stream to)]
    (io/copy in out)))

(defn download-urls [urls dir]
  (let [furls (map io/file urls)
        names (map #(.getName ^java.io.File %) furls)
        files (map io/file (repeat dir) names)]
    (println "Downloading files...")
    (doall (pmap copy urls files names))))

;;; download
(defmulti download :agency)

(defmethod download :msha [agency dir]
  (let [urls (-> agency :data vals)]
    (download-urls urls dir)))

(defmethod download :bls [agency dir]
  (let [site (:data-main agency)
        regs (-> agency :data vals)
        ends (prep-urls site regs)
        urls (map #(str (:home BLS) %) ends)]
    (download-urls urls dir)))

(defmethod download :epa [agency dir]
  (let [site (:data-main agency)
        dat (:data agency)
        regs (into (-> dat :gases vals) (-> dat :meterological vals))
        ends (prep-urls site regs)
        urls (map #(str (:data-base agency) %) ends)]
    (download-urls urls dir)))

(defmethod download :noaa [agency dir]
  (let [urls (-> agency :data vals)
        files (map io/file (repeat dir) urls)]
    (println "Downloading files...")
    (ftp/with-ftp [client (agency :client)]
      (doall (map #(ftp/client-get client %1 %2) urls files)))))


;;; main
(def cli-options
  [["-h" "--help" "print this message and exit"]
   ["-a" "--agency AGENCY" "acronym of agency of interest"
    :parse-fn #(string/upper-case (str %))]
   ["-f" "--folder FOLDER" "folder to store agency data, absolute path"
    :parse-fn #(str %)]])

(defn usage [options-summary]
  (->> ["federal_data => analysis ready\n"
        "Usage: java -jar fd.jar action -a AGENCY -f FOLDER\n"
        "Actions:"
        "  available\t list of agencies federal_data plays nicely with"
        "  download\tdownload agency data"
        "\nArguments:"
        options-summary
        "\nSee README for more information."]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ;; Handle help and error conditions
    (cond
     (options :help) (exit 0 (usage summary))
     errors (exit 1 (error-msg errors)))
    (println "User specified options" options)
    (let [act (-> arguments first (nth 0) str)
          agency (:agency options)]
     (case act
       "d" (download (agencies agency) (:folder options))
       "a" (available agency)
       "no action specified")))
  (shutdown-agents)
  (exit 0 "done."))






;; gBoepBxVfRbWUJNeufBogzeVGcEQMscQ
