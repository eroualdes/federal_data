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

(ns federal-data.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]))

(def cli-options
  [["-h" "--help" "print this message and exit"]
   ["-a" "--agency AGENCY" "acronym of agency of interest"
    :parse-fn #(string/lower-case (str %))]
   ["-f" "--folder FOLDER" "folder to store agency data, absolute path"
    :parse-fn #(str %)]])

(defn usage [options-summary]
  (->> ["federal_data => analysis ready\n"
        "Usage: java -jar fd.jar action -a AGENCY -f FOLDER\n"
        "Actions:"
        "  available\t list of agencies federal_data plays nicely with"
        "  download\t download agency data"
        "  help\t\t print this message and exit"
        "\nArguments:"
        options-summary
        "\nSee README for more information."]
       (string/join \newline)))

(defn error-msg [error]
  (str "The following errors occurred while parsing your command:\n"
       error))

(defn error-msgs [errors]
  (str "The following errors occurred while parsing your command:\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn get-opts [args]
  (parse-opts args cli-options))
