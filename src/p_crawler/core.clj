(ns p-crawler.core
  "This is the primary ."
  (:gen-class)
  (:require [p-crawler.database :refer :all]
            [p-crawler.classifier :refer :all]
            [p-crawler.crawler :refer :all]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (crawl-web (or (retrieve-queue) args)
             (partial classify-domain! :pornography?)))
