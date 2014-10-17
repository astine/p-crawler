(defproject p-crawler "0.1.0-SNAPSHOT"
  :description "Web crawler for seeking out objectionable material"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-robots "0.6.0"]
                 [clj-time "0.9.0-beta1"]
                 [clj-http "0.9.2"]
                 [clojurewerkz/urly "1.0.0"]
                 [com.taoensso/timbre "3.3.1"]
                 [com.novemberain/pantomime "2.3.0"]
                 [com.novemberain/monger "2.0.0"]
                 [enlive "1.1.5"]]
  :resource-paths ["resources/codox-info-0.1.1-SNAPSHOT.jar"]
  :plugins [[codox "0.8.10"]
            [codox-info "0.1.1-SNAPSHOT"]]
  :codox {:writer codox-info.core/write-to-info}
  :main ^:skip-aot p-crawler.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
