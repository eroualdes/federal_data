;   Copyright (c) 2015 Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns fd.Agencys
  (:require [clojure.string :as string]))

(def MSHA
  {:agency :msha, :conn :http
   :home "http://www.msha.gov/STATS/PART50/p50y2k/p50y2k.HTM",
   :docs "http://www.msha.gov/STATS/PART50/P50Y2K/P50Y2KHB.PDF",
   :data-main "http://www.msha.gov/OpenGovernmentData/OGIMSHA.asp",
   :data-base "http://www.msha.gov/OpenGovernmentData/DataSets/",
   :data {:accident_injury "Accidents.zip",
          :area "AreaSamples.zip",
          :coal_dust "CoalDustSamples.zip",
          :conferenes "Conferences.zip",
          :civil_penalty "CivilPenaltyDocketsAndDecisions.zip",
          :employment_production "MinesProdQuarterly.zip",
          :inspections "Inspections.zip",
          :mine_addresses "AddressofRecord.zip",
          :mines "Mines.zip",
          :noise "NoiseSamples.zip",
          :health "PersonalHealthSamples.zip",
          :quartz "QuartzSamples.zip",
          :violations "Violations.zip",
          :contested_violations "ContestedViolations.zip",
          :assessed_violations "AssessedViolations.zip"}})

(def BLS
  {:agency :bls, :conn :http,
   :home "http://www.bls.gov",
   :docs "http://www.bls.gov/cew/doctoc.htm",
   :data-main "http://www.bls.gov/cew/datatoc.htm",
   :data {:naics #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_naics10_totals.zip",
          :industry #"/cew/data/files/\d{4}/csv/(\d{4})_qtrly_by_industry.zip"}})

(def NOAA
  {:agency :noaa, :conn :ftp,
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
          :countries "ghcnd-countries.txt"}})

(def EPA
  {:agency :epa, :conn :http,
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

(def Agencys
  {"msha" MSHA, "bls" BLS, "noaa" NOAA, "epa" EPA})

(defn Agency? [Ads]
  "Does fd know how to work with user specified Agency:dataset?"
  (let [sp (string/split Ads #":")
        ag (first sp) Ag (Agencys ag)
        ds (rest sp) Ds (mapv keyword ds)]
    [Ag Ds]))

(defn fselect-keys [map keyseq]
  (let [m (select-keys map keyseq)]
    (if (empty? m) map m)))

(defn process-agency [Ads]
  "With user specified Agency:dataset return Agency with correct data."
  (if-let [v (Agency? Ads)]
    (let [Ag (first v)]
      (merge (dissoc Ag :data)
             {:data (fselect-keys (:data Ag) (last v))}))))
