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

(ns federal-data.available
  (:require [clojure.string :as string]
            [federal-data.Agencys :as A]))

(defn get-available [agency]
  "Return vector of agency's available data."
  (mapv name (-> agency :data keys)))

(defn available
  "Print available data."
  ([]
     (let [ags (string/join ", " (keys A/Agencys))]
      (println "federal_data from the following agencies:" ags)))
  ([agency]
     (if (nil? agency)
       (available)
       (let [ags (A/Agencys agency)
             datasets (string/join ", " (get-available ags))]
         (println (string/upper-case agency) "contains data sets:" datasets)))))
