(ns p-crawler.database
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [clojure.string :refer [join]]
            [clojure.core.async :refer [go thread chan put! <!! <!] :as async]
            [p-crawler.transducers :refer :all]))

(def db (mg/get-db (mg/connect) "p-crawler"))

(def cache "In-memory database cache" (atom {}))

;; Cache management functions

(defn add-collection-to-cache! [collection]
  (swap! cache assoc collection (atom {})))

(defn remove-collection-from-cache! [collection]
  (swap! cache dissoc collection))

(defn get-collection-from-cache [collection]
  (or (get @cache collection)
      (add-collection-to-cache! collection)))

(defn add-document-to-cache! [collection doc-name document]
  (swap! (get-collection-from-cache collection)
         assoc doc-name document))

(defn remove-document-from-cache! [collection doc-name]
  (swap! (get-collection-from-cache collection)
         dissoc doc-name))

(defn get-document-from-cache [collection doc-name]
  (get (get-collection-from-cache collection) doc-name))

(defn query-document-in-cache [collection selkeys]
  (get-in (get-collection-from-cache collection)
          selkeys))

(defn update-document-in-cache! [collection selkeys value]
  (swap! (get-collection-from-cache collection)
         assoc-in selkeys value))

;; Database interaction functions

(defn- dotted-key
  ([keys]
     (join "." (map name keys)))
  ([k & keys]
     (dotted-key (cons k keys))))

(def update-chan (chan 10000 (comp (partitioning-all 20)
                                   (mapcatting set))))

(defn run-updates! [[ioc?]]
    (if ioc?
      (go
       (loop [[collection [doc-name & selkeys] value :as update]
              (<! update-chan)]
         (when update
           (mc/update-by-id db collection doc-name
                            (array-map $set (array-map (dotted-key selkeys) value))
                            {:upsert true})
           (recur (<! update-chan)))))
     (thread (loop [[collection [doc-name & selkeys] value :as update]
                    (<!! update-chan)]
               (when update
                 (mc/update-by-id db collection doc-name
                                  (array-map $set (array-map (dotted-key selkeys) value))
                                  {:upsert true})
                 (recur (<!! update-chan)))))))

(defn update-document! [collection selkeys value]
  (put! update-chan [collection selkeys value])
  (update-document-in-cache! collection selkeys value))

(defn get-document [collection doc-name]
  (or (get-document-from-cache collection doc-name)
      (mc/find-map-by-id db collection doc-name)))

(defn query-document [collection [doc-name & selkeys]]
  (or (query-document-in-cache collection (cons doc-name selkeys))
      (get-in (mc/find-map-by-id db collection doc-name [(dotted-key selkeys)])
              selkeys)))