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

(defproject federal_data "0.1.0"
  :description "analysis ready federal data."
  :url "http://roualdes.us/federal_data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [http-kit "2.1.16"]
                 [com.velisco/clj-ftp "0.3.3"]]
  :plugins [[cider/cider-nrepl "0.8.2"]]
  :main ^:skip-aot federal-data.core
  :profiles {:uberjar {:aot :all
                       :uberjar-name "fd.jar"}})
