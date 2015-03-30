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

(ns federal-data.Agencys
  (:require [federal-data.cli :as cli]
            [clojure.string :as string]))

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

(def Agencys
  {"msha" MSHA, "bls" BLS, "noaa" NOAA, "epa" EPA})

(defn Agency? [Ads]
  "Does fd know how to work with user specified Agency:dataset?"
  (let [sp (string/split Ads #":")
        ag (first sp) Ag (Agencys ag)
        ds (last sp) Ds (keyword ds)
        emsg (format "fd doesn't know how to work with %s or %s" ag ds)]
    (if (and Ag (contains? (Ag :data) Ds))
      [Ag Ds]
      (cli/exit 1 (cli/error-msg emsg)))))

(defn fselect-keys [map keyseq]
  (let [m (select-keys map keyseq)]
    (if (empty? m) map m)))

(defn process-agency [Ads]
  "With user specified Agency:dataset return Agency with correct data."
  (if-let [v (Agency? Ads)]
    (let [Ag (first v)]
      (merge (dissoc Ag :data)
             {:data (fselect-keys (:data Ag) [(last v)])}))))
