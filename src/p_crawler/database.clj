(ns p-crawler.database
  (:require [monger.core :as mg]))

(def db (mg/get-db (mg/connect) "p-crawler"))

(def cache (atom {}))

(defn add-collection! [collection]
  (swap! cache assoc collection (atom {})))

(defn remove-collection! [collection]
  (swap! cache dissoc collection))

(defn get-collection [collection]
  (or (get @cache collection)
      (add-collection! collection)))

(defn add-document! [collection doc-name document]
  (swap! (get-collection collection)
         assoc doc-name document))

(defn remove-document! [collection doc-name]
  (swap! (get-collection collection)
         dissoc doc-name))

(defn get-document [collection doc-name]
  (get (get-collection collection) doc-name))

(defn update-document! [collection doc-name document]
  (swap! (get-collection collection)
         assoc doc-name document))