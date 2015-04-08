;   Copyright (c) 2015 Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns fd.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [fd.Agencys :as A]
            [fd.download :refer [download]]
            [fd.available :refer [available]]
            [fd.cli :as cli])
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
       "d" (do (println "Flags" options)
               (download (A/process-agency ag) fldr))
       "a" (available ag)
       "t" (println ((A/Agencys ag) :data))
       "No action specified.")))
  (shutdown-agents)
  (cli/exit 0 ""))






;; gBoepBxVfRbWUJNeufBogzeVGcEQMscQ
