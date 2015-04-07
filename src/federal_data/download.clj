;   Copyright (c) Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns federal-data.download
  (:require [clojure.java.io :as io]
            [org.httpkit.client :as http]
            [miner.ftp :as ftp]))

(def ^:dynamic *d-cntr* "Counter for downloading files." (atom 0))
(def ^:dynamic *d-tot* "Total number of files to download." (atom 0))

(defn dl-ing []
  "Update user that downloading is happening by printing to console."
  (printf "\rDownloading files... %s/%s." @*d-cntr* @*d-tot*)
  (flush))

(defn copy [from to]
  "Copy file into from/to specified locations."
  (with-open [in (io/input-stream from)
              out (io/output-stream to)]
    (io/copy in out))
  (swap! *d-cntr* inc)
  (dl-ing))

(defn find-urls
  "Return list of urls by scanning agency's sites 
  with agency's regexed-urls."
  [site regs]
  (let [content (:body @(-> site http/get))]
    (loop [r regs
           result []]
      (if (empty? r)
        result
        (recur (rest r)
               (into result (mapv first (re-seq (first r) content))))))))

(defmulti prep "Prepare agency's urls." :agency)

(defmethod prep :msha [agency]
  (let [ends (-> agency :data vals)
        urls (map #(str (:data-base) %) ends)]
    urls))

(defmethod prep :bls [agency]
  (let [site (:data-main agency)
        regs (-> agency :data vals)
        ends (find-urls site regs)
        urls (map #(str (:home agency) %) ends)]
    urls))

(defmethod prep :epa [agency]
  (let [site (:data-main agency)
        dat (:data agency)
        regs (into (-> dat :gases vals)
                   (-> dat :meterological vals))
        ends (find-urls site regs)
        urls (map #(str (:data-base agency) %) ends)]
    urls))

(defmethod prep :noaa [agency]
  (let [urls (-> agency :data vals)
        nurls (swap! *d-tot* + (count urls))]
    urls))

(defmulti download :conn)

(defn download-urls-http [urls dir]
  "Download urls into dir from http connection."
  (let [nurls (swap! *d-tot* + (count urls))
        furls (map io/file urls)
        names (map #(.getName ^java.io.File %) furls)
        files (map io/file (repeat dir) names)]
    (dl-ing)
    (doall (pmap copy urls files))))

(defmethod download :http [agency dir]
  "http/download agency's data into dir."
  (download-urls-http (prep agency) dir))

(defn ftp-get [client]
  "Return function that downloads from ftp and informs user about status."
  (let [cl client]
    (fn [x y]
      (ftp/client-get cl x y)
      (swap! *d-cntr* inc)
      (dl-ing))))

(defn download-urls-ftp [client urls files]
  "Download urls into files from ftp connection."
  (dl-ing)
  (ftp/with-ftp [cl client]
    (let [dl (ftp-get cl)]
      (doall (map dl urls files)))))

(defmethod download :ftp [agency dir]
  "ftp/download agency's data into dir."
  (let [urls (prep agency)
        files (map io/file (repeat dir) urls)
        client (agency :client)]
    (download-urls-ftp client urls files)))
