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
            [federal-data.Agencys :as A]
            [federal-data.download :refer [download]]
            [federal-data.available :refer [available]]
            [federal-data.cli :as cli])
  (:gen-class))
(set! *warn-on-reflection* true)

(defn -main [& args]
  (let [gopts (cli/get-opts args)
        {:keys [options arguments errors summary]} gopts]
    ;; Handle help and error conditions
    (cond
      (options :help) (cli/exit 0 (cli/usage summary))
      errors (cli/exit 1 (cli/error-msgs errors)))
    (let [act (-> arguments first (nth 0) str)
          ag (:agency options)
          fldr (:folder options)]
     (case act
       "d" (do (println "Flags" options) (download (A/process-agency ag) fldr))
       "a" (do (println "Flags" options) (available ag))
       "h" (cli/exit 0 (cli/usage summary))
       "No action specified.")))
  (shutdown-agents)
  (cli/exit 0 ""))






;; gBoepBxVfRbWUJNeufBogzeVGcEQMscQ
