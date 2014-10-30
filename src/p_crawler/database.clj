(ns p-crawler.database
  (:require [monger.core :as mg]))

(def db (mg/get-db (mg/connect) "p-crawler"))
