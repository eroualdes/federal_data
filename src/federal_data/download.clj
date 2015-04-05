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

(def ^:dynamic *d-cntr* "Counter for downloading files" (atom 0))
(def ^:dynamic *d-tot* "Total number of files to download" (atom 0))

(defn dl-ing []
  "Update user that downloading is happening by printing to console."
  (printf "\rDownloading files... %s/%s." @*d-cntr* @*d-tot*)
  (flush))

(defn copy [from to]
  (with-open [in (io/input-stream from)
              out (io/output-stream to)]
    (io/copy in out))
  (swap! *d-cntr* inc)
  (dl-ing))

(defn download-urls [urls dir]
  (let [nurls (swap! *d-tot* + (count urls))
        furls (map io/file urls)
        names (map #(.getName ^java.io.File %) furls)
        files (map io/file (repeat dir) names)]
    (dl-ing)
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

(defn ftp-get [client]
  (let [cl client]
    (fn [x y]
      (ftp/client-get cl x y)
      (swap! *d-cntr* inc)
      (dl-ing))))

(defmethod download :noaa [agency dir]
  (let [urls (-> agency :data vals)
        nurls (swap! *d-tot* + (count urls))
        files (map io/file (repeat dir) urls)]
    (dl-ing)
    (ftp/with-ftp [client (agency :client)]
      (let [dl (ftp-get client)]
        (doall (map dl urls files))))))
