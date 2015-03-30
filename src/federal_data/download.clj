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

(ns federal-data.download
  (:require [clojure.java.io :as io]
            [org.httpkit.client :as http]
            [miner.ftp :as ftp]))

(def ^:dynamic *d-cntr* "Counter for downloading files" (atom 0))
(def ^:dynamic *d-tot* "Total number of files to download" (atom 0))

(defn copy [from to]
  (with-open [in (io/input-stream from)
              out (io/output-stream to)]
    (io/copy in out))
  (swap! *d-cntr* inc)
  (printf "\rDownloading files... %s/%s." @*d-cntr* @*d-tot*)
  (flush))

(defn download-urls [urls dir]
  (let [nurls (swap! *d-tot* + (count urls))
        furls (map io/file urls)
        names (map #(.getName ^java.io.File %) furls)
        files (map io/file (repeat dir) names)]
    (printf "\rDownloading files... %s/%s." @*d-cntr* @*d-tot*)
    (flush)
    (doall (pmap copy urls files))))

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
               (into result (mapv first (re-seq (first r) content))))))))

(defmulti download :agency)

(defmethod download :msha [agency dir]
  (let [urls (-> agency :data vals)]
    (download-urls urls dir)))

(defmethod download :bls [agency dir]
  (let [site (:data-main agency)
        regs (-> agency :data vals)
        ends (prep-urls site regs)
        urls (map #(str (:home agency) %) ends)]
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
        nurls (swap! *d-tot* + (count urls))
        files (map io/file (repeat dir) urls)]
    (ftp/with-ftp [client (agency :client)]
      (doall (map #(ftp/client-get client %1 %2) urls files)))))
