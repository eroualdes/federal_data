(defproject federal_data "0.1.0"
  :description "Download and combine federal data into analysis ready data."
  :url "http://roualdes.us/federal_data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-http "1.0.1"]]
  :plugins [[cider/cider-nrepl "0.8.2"]]
  :main ^:skip-aot federal-data.core
  :profiles {:uberjar {:aot :all
                       :uberjar-name "fd.jar"}}
  :global-vars {*print-length* 100
                *warn-on-reflection* true
                *assert* false})
