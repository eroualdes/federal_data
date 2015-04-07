;   Copyright (c) Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns federal-data.core-test
  (:require [clojure.test :as ct]
            [federal-data.download :as dl]
            [federal-data.Agencys :as A]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))
;;; will URLs work? does this have further implications? file type?

(defn check-urls [xs]
  (map #(:header @(-> % http/get)) xs))

(deftest urls-exist
  (testing "BLS"
    (true? (not (empty? )))))


;;; process-agency given a Agency:dataset (Ads) specification?
