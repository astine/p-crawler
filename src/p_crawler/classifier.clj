(ns p-crawler.classifier
  (:require [monger.core :as mg]
            [monger.query :as mq]
            [monger.joda-time :refer :all]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [difference intersection union]]
            [clojure.core.async :refer [go]]
            [p-crawler.database :refer :all]
            [p-crawler.crawler :refer :all])
  (:import [java.net URL]))

(def classifiers (atom {}))

(defn save-classifier! [{:keys [category] :as classifier}]
  (update-document! "classifiers" [category] classifier))

(defn generate-classifier [category]
  {:category category
   :count 0
   :probabilities {}
   :anti-probabilities {}} )

(defn classifier [category]
  (or (get-document "classifiers" category)
      (update-document! "classifiers" [category]
                        (generate-classifier category))))

(defn update-classifier! [category fn]
  (-> (classifier category)
      fn
      save-classifier!))
  
(defn increment-token-probability [prior prior-count]
  (/ (+ (* prior prior-count) 1)
     (inc prior-count)))

(defn decrement-token-probability [prior prior-count]
  (/ (+ (* prior prior-count) 0)
     (inc prior-count)))

(defn process-document-tokens [{:keys [probabilities anti-probabilities count]} tokens match?]
  (let [old-tokens (set (keys probabilities))
        new-tokens (difference tokens old-tokens)
        incrementing-tokens (intersection tokens old-tokens)
        decrementing-tokens (difference old-tokens tokens)
        inc-probs (transient (if match? probabilities anti-probabilities))
        dec-probs (transient (if match? anti-probabilities probabilities))]
    (doseq [token new-tokens]
      (assoc! inc-probs token (/ 1 (inc count)))
      (assoc! dec-probs token 0))
    (doseq [token incrementing-tokens]
      (assoc! inc-probs
              token
              (increment-token-probability (get inc-probs token) count)))
    (doseq [token decrementing-tokens]
      (assoc! dec-probs
              token
              (decrement-token-probability (get dec-probs token) count)))
    {:count (int count)
     :probabilities (persistent! (if match? inc-probs dec-probs))
     :anti-probabilities (persistent! (if match? dec-probs inc-probs))}))
    
(defn train-classifier [classifier matches antimatches]
  (let [reduce-update (partial reduce process-document-tokens)]
    (-> classifier
        (reduce-update matches (repeat true))
        (reduce-update antimatches (repeat false)))))

(defn- dotted-key
  ([keys]
     (clojure.string/join "." (map name keys)))
  ([k & keys]
     (dotted-key (cons k keys))))

(defn fetch-classified-domains [category matched? count]
  (mq/with-collection db "domains"
    (mq/find {(dotted-key :manual_class category) (if matched? 1 0)})
    (mq/fields [:domain :tokens])
    (mq/sort (array-map (dotted-key :manual_class category) (if matched? -1 1)))
    (mq/limit count)))
  
