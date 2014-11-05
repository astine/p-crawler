(ns p-crawler.database
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.joda-time :refer :all]
            [clj-time.core :as time]
            [clojure.string :refer [join]]
            [clojure.core.async :refer [go thread chan put! <!! <!] :as async]
            [p-crawler.transducers :refer :all]))

(def ^:private db (mg/get-db (mg/connect) "p-crawler"))

(def ^:private cache "In-memory database cache" (atom {}))

(def ^:private cache-last-update-key (keyword (gensym "last-cache-update-")))

;; Cache management functions

(defn- add-collection-to-cache! [collection]
  (swap! cache assoc collection (atom {})))

(defn- remove-collection-from-cache! [collection]
  (swap! cache dissoc collection))

(defn- get-collection-from-cache [collection]
  (or (get @cache collection)
      (get (add-collection-to-cache! collection) collection)))

(defn- add-document-to-cache! [collection doc-name document]
  (swap! (get-collection-from-cache collection)
         assoc doc-name (assoc document cache-last-update-key (time/now))))

(defn- remove-document-from-cache! [collection doc-name]
  (swap! (get-collection-from-cache collection)
         dissoc doc-name))

(defn- get-document-from-cache [collection doc-name]
  (get @(get-collection-from-cache collection) doc-name))

(defn- query-document-in-cache [collection selkeys]
  (get-in @(get-collection-from-cache collection)
          selkeys))

(defn- update-document-in-cache! [collection [doc-name & selkeys] value]
  (get-in (if selkeys
            (swap! (get-collection-from-cache collection)
                   #(assoc % doc-name (assoc (assoc-in (get % doc-name) selkeys value)
                                        cache-last-update-key (time/now))))
            (add-document-to-cache! collection doc-name value))
          (cons doc-name selkeys)))

;; Database interaction functions

(defn- dotted-key
  ([keys]
     (join "." (map name keys)))
  ([k & keys]
     (dotted-key (cons k keys))))

(def  ^:private update-chan (chan 10000 (comp (partitioning-all 20)
                                   (mapcatting set))))

(defn- update-db! [collection [doc-name & selkeys] value]
  (mc/update-by-id db collection doc-name
                   (if (not-empty selkeys)
                     (array-map $set (array-map (dotted-key selkeys) value))
                     value)
                   {:upsert true}))
  
(defn run-updates! [& [ioc?]]
  (if ioc?
    (go
     (loop [[collection selkeys value :as update]
            (<! update-chan)]
       (when update
         (update-db! collection selkeys value)
         (recur (<! update-chan)))))
    (thread
     (loop [[collection selkeys value :as update]
            (<!! update-chan)]
       (when update
         (update-db! collection selkeys value)
         (recur (<!! update-chan)))))))

(defn update-document! [collection selkeys value]
  (put! update-chan [collection selkeys value])
  (update-document-in-cache! collection selkeys value))

(defn get-document [collection doc-name]
  (or (get-document-from-cache collection doc-name)
      (get (swap! (get-collection-from-cache collection)
                  assoc doc-name (dissoc (assoc (mc/find-map-by-id db collection doc-name)
                                           cache-last-update-key (time/now))
                                         :_id))
           doc-name)))

(defn query-document [collection [doc-name & selkeys]]
  (or (query-document-in-cache collection (cons doc-name selkeys))
      (get-in (swap! (get-collection-from-cache collection)
                     assoc doc-name (dissoc (assoc (mc/find-map-by-id db collection doc-name)
                                              cache-last-update-key (time/now))
                                            :_id))
              selkeys)))

;; Cache garbage collector

(def ^:private max-cache-entries 10000)

(defn- get-items-to-remove []
  (->> @cache
       (mapcat #(map (fn [doc] (cons (first %) doc)) @(second %)))
       (sort-by (comp cache-last-update-key last))
       (reverse)
       (#(nthrest % max-cache-entries))
       (map butlast)
       (group-by first)))

(defn- trim-excess-cache-entries! []
  (let [items (get-items-to-remove)]
    (doseq [[collection documents] items]
      (swap! (get-collection-from-cache collection)
             #(apply dissoc (cons % (map second documents))))))) 

(defn run-cache-gc! []
  (go (loop []
        (trim-excess-cache-entries!)
        (<!! (async/timeout 100))
        (recur))))
