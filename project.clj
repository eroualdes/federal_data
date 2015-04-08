;   Copyright (c) 2015 Edward A. Roualdes. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(defproject fd "0.1.0"
  :description "analysis ready federal data."
  :url "http://roualdes.us/fd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-http-lite "0.2.1"]
                 [com.velisco/clj-ftp "0.3.3"]]
  :plugins [[cider/cider-nrepl "0.8.2"]]
  :main ^:skip-aot fd.core
  :profiles {:uberjar {:aot :all
                       :uberjar-name "fd.jar"}})
