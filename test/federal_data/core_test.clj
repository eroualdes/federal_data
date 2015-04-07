;   Copyright (c) Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns federal-data.core-test
  (:require [clojure.test :refer :all]
            [federal-data.download :as dl]
            [federal-data.Agencys :as A]
            [clj-http.lite.client :as http]))

;;; do URLs work? does this have further implications? file type?
(defn check-url [url]
  "Check url without downloading body."
  (let [status (:status (http/head url))]
    (if (= 200 status)
      true
      false)))

(defmacro check-agency [ag]
  "Check all urls for Agency ag such that each failed url is print to stderr."
  `(testing '~ag
     (are [url] (true? (check-url url))
          ~@(dl/prep (A/Agencys ag)))))

(deftest URLs-exist?
  (check-agency "msha")
  (check-agency "bls")
  (check-agency "epa"))

;;; process-agency given a Agency:dataset (Ads) specification?


