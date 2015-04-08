;   Copyright (c) 2015 Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns fd.available
  (:require [clojure.string :as string]
            [fd.Agencys :as A]))

(defn get-available [agency]
  "Return vector of agency's available data."
  (mapv name (-> agency :data keys)))

(defn available
  "Print available data."
  ([]
   (let [ags (->> (keys A/Agencys)
                 (map string/upper-case)
                 (string/join ", "))]
      (println "Federal data available from the following agencies:" ags)))
  ([agency]
     (if (nil? agency)
       (available)
       (let [ags (A/Agencys agency)
             datasets (string/join ", " (get-available ags))]
         (println (string/upper-case agency) "contains data sets:" datasets)))))
